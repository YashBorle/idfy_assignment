import apache_beam as beam
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime, timedelta
from typing import Dict, Any, Iterable, Tuple
import argparse
import os
from apache_beam.transforms.userstate import BagStateSpec, ReadModifyWriteStateSpec
import apache_beam.coders as coders
from apache_beam.io.gcp.bigquery import WriteToBigQuery



class KeyBySession(beam.DoFn):
    def process(self, event):
        sid = event.get("session_id") or f"test:{event.get('test_id')}_student:{event.get('student_id')}"
        yield (sid, event)

class ParseJsonDoFn(beam.DoFn):
    def process(self, element):
        """
        element: a line (JSON)
        """
        try:
            raw = json.loads(element)
        except Exception as ex:
            # In production send to DLQ
            print(ex)
            return
        norm = normalize_event(raw)
        yield norm


class DeduplicateEvents(beam.DoFn):
    """
    Deduplicates events per (event_id).
    Uses Beam per-key state to remember seen event_ids.
    """

    SEEN_IDS = BagStateSpec("seen_ids",coder=coders.StrUtf8Coder())

    def process(
        self,
        element,
        seen_state=beam.DoFn.StateParam(SEEN_IDS),
    ):
        key, event = element
        event_id = event.get("event_id")

        # If already seen, drop it
        if event_id in seen_state.read():
                return

        # Mark this ID as seen
        seen_state.add(event_id)

        yield event




def parse_ts(ts: str) -> datetime:
    """Parse ISO-like timestamp with optional fractional seconds and Z suffix."""
    if ts is None:
        return None
    s = ts
    if s.endswith("Z"):
        s = s[:-1]
    # Try multiple formats
    fmts = ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S")


    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            continue
    # last resort
    return datetime.fromisoformat(s)

def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return normalized event dict with parsed timestamps and expected keys."""
    e = dict(raw)  # shallow copy
    e.setdefault("event_id", None)
    e.setdefault("event_type", None)
    e.setdefault("test_id", None)
    e.setdefault("student_id", None)
    e.setdefault("question_id", None)
    e.setdefault("session_id", None)
    e.setdefault("attempt_no", None)
    e.setdefault("answer", None)
    e.setdefault("is_correct_answer", None)
    # parse times
    e["event_time"] = parse_ts(e.get("client_ts"))
    del e["client_ts"]
    return e


def sessionize_events(session_id, events) -> Dict[str, Any]:
    """Takes events for a session_id and returns a session summary dict."""
    evs = sorted([e for e in events if e.get("event_time") is not None], key=lambda x: x["event_time"])
    if not evs:
        return None
    start = evs[0]["event_time"]
    end = evs[-1]["event_time"]
    duration = int((end - start).total_seconds())
    questions_viewed = len(set([e.get("question_id") for e in evs if e.get("question_id")]))
    questions_submitted = sum(1 for e in evs if e.get("event_type") in ("answer_submit", "question_submit"))
    # TTL expiration: if inactivity > 30 minutes from last event considered expired? For demo, we check simple rule:
    expired = 0
    TTL = timedelta(minutes=30)
    # if last event is more than TTL after start (odd heuristic) -> not expired; better: if any gap > TTL -> expired. We'll mark expired if any gap > TTL.
    for i in range(1, len(evs)):
        gap = evs[i]["event_time"] - evs[i-1]["event_time"]
        if gap > TTL:
            expired = 1
            break
    session_summary = {
        "session_id": session_id,
        "test_id": evs[0].get("test_id"),
        "student_id": evs[0].get("student_id"),
        "session_start": start.isoformat(),
        "session_end": end.isoformat(),
        "duration_seconds": duration,
        "questions_viewed": questions_viewed,
        "questions_submitted": questions_submitted,
        "expired": expired,
        "event_day": start.date().isoformat()
    }
    return session_summary



def run(argv=None):

    parser = argparse.ArgumentParser()
    # parser.add_argument("--input", required=True, help="Input JSONL file")
    # parser.add_argument("--output_dir", required=True, help="Output directory")
    known_args, beam_args = parser.parse_known_args(argv)

    output_dir = "./"
    # output_dir = known_args.output_dir

    pipeline_options = PipelineOptions(beam_args, save_main_session=True)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline() as pipeline:
        parsed = (
            pipeline
            # | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)
            | 'ReadFromText' >> beam.io.ReadFromText('mcq_test_sample_data_with_errors.jsonl')
            | 'ParseInput' >> beam.ParDo(ParseJsonDoFn())
            | 'AddProcessTs' >> beam.Map(lambda x : {**x,**{"process_ts":str(datetime.now())}})
            # datetime.strptime(,'%Y-%m-%dT%H:%M:%S')}})
        )

        dedup_data = (
            parsed
            | 'KeyByStudentId' >> beam.Map(lambda x : (x['student_id'],x))
            | 'DedupByKey' >> beam.ParDo(DeduplicateEvents())
            # | 'Print' >> beam.Map(print)
        )

        # I would this data into warehouse and create get other facts out of this data as i have mentioned about
        _ = (
            dedup_data                
                | 'DTToStr' >> beam.Map(lambda x : {**x,**{"event_time":str(x.get("event_time"))}})
                | 'DumpJson' >> beam.Map(lambda x : json.dumps(x))
                | "WriteEventsFact" >> beam.io.WriteToText(os.path.join(output_dir, "fact_events.json"), num_shards=1)
        )



        sessions = (
            dedup_data
            | "KeyBySession" >> beam.Map(lambda x : (x.get('session_id'),x))
            | "GroupBySession" >> beam.GroupByKey()
            | "BuildSessionSummary" >> beam.MapTuple(lambda sid, evs: sessionize_events(sid, evs))
            | "FilterNoneSessions" >> beam.Filter(lambda x: x is not None)
            | "ToSessionJson" >> beam.Map(lambda s: json.dumps(s))
        )
        # This is an example if we want to sample the session_fact and do some aggregations in the pipeline | We output this file and load into our warehouse
        sessions | "WriteSessions" >> beam.io.WriteToText(os.path.join(output_dir, "fact_sessions.json"), num_shards=1, skip_if_empty=True)



if __name__ == "__main__":
    run()