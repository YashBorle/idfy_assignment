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
    e.setdefault("is_correct", None)
    e.setdefault("client_ts", None)
    e.setdefault("server_ts", None)
    # parse times
    e["_client_dt"] = parse_ts(e.get("client_ts"))
    e["event_time"] = e["_client_dt"]
    return e




def run(argv=None):

    parser = argparse.ArgumentParser()
    # parser.add_argument("--input", required=True, help="Input JSONL file")
    # parser.add_argument("--output_dir", required=True, help="Output directory")
    known_args, beam_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(beam_args, save_main_session=True)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline() as pipeline:
        parsed = (
            pipeline
            # | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)
            | 'ReadFromText' >> beam.io.ReadFromText('sample_event_data.txt')
            | 'ParseInput' >> beam.ParDo(ParseJsonDoFn())
        )

        dedup_data = (
            parsed
            | 'KeyByStudentId' >> beam.Map(lambda x : (x['student_id'],x))
            | 'DedupByKey' >> beam.ParDo(DeduplicateEvents())
            | 'Print' >> beam.Map(print)
            # | "WriteEventsFact" >> beam.io.WriteToText(os.path.join(output_dir, "fact_events.jsonl"), num_shards=1)
        )



if __name__ == "__main__":
    run()