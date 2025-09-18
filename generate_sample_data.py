import random
import uuid
import json
from datetime import datetime, timedelta

# Constants
event_types = ['session_start', 'session_end', 'page_view', 'answer_submit', 'answer_resubmit', 'test_submit', 'store_error']
questions = [f"Q{i}" for i in range(1, 100)]  # Assume 100 questions in the test
students = [f"S{i}" for i in range(1, 1001)]  # Assume 1000 students
tests = [f"T{i}" for i in range(1, 51)]  # Assume 50 tests

# Function to generate unique UUIDs for event_id and session_id
def generate_uuid():
    return str(uuid.uuid4())

# Function to generate random timestamps
def generate_random_timestamp(start_date, end_date):
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

# Function to decide whether to trigger a store error (5% chance)
def should_trigger_store_error():
    return random.random() < 0.05

# Sample data
records = []
start_date = datetime(2025, 9, 1)
end_date = datetime(2025, 9, 30)

# Open file in write mode to create JSONL format
with open('mcq_test_sample_data_with_errors.jsonl', 'w') as f:
    # Generate 100,000 rows of data
    for _ in range(100000):
        event_id = generate_uuid()
        event_type = random.choice(event_types)
        test_id = random.choice(tests)
        student_id = random.choice(students)
        question_id = random.choice(questions)
        session_id = test_id+"_"+student_id+str(random.randint(1, 3))
        
        # Randomizing the event data
        if event_type == 'session_start':
            answer = None
            is_correct_answer = None
            attempt_no = None
        elif event_type == 'session_end':
            answer = None
            is_correct_answer = None
            attempt_no = None
        elif event_type == 'page_view':
            answer = None
            is_correct_answer = None
            attempt_no = None
        elif event_type in ['answer_submit', 'answer_resubmit']:
            answer = random.choice(['A', 'B', 'C', 'D', 'E'])
            is_correct_answer = random.choice([True, False])
            attempt_no = random.randint(1, 3)
            # Simulate store error after submit/resubmit with 5% probability
            if should_trigger_store_error():
                event_type = 'store_error'
                answer = None
                is_correct_answer = None
                attempt_no = None
        elif event_type == 'test_submit':
            answer = None
            is_correct_answer = None
            attempt_no = None
            # Simulate store error during test submission with 5% probability
            if should_trigger_store_error():
                event_type = 'store_error'
                answer = None
                is_correct_answer = None
                attempt_no = None
        elif event_type == 'store_error':
            # Simulate store error
            answer = None
            is_correct_answer = None
            attempt_no = None

        client_ts = generate_random_timestamp(start_date, end_date)

        # Create the record as a dictionary
        record = {
            'event_id': event_id,
            'event_type': event_type,
            'test_id': test_id,
            'student_id': student_id,
            'question_id': question_id,
            'session_id': session_id,
            'answer': answer,
            'is_correct_answer': is_correct_answer,
            'attempt_no': attempt_no,
            'client_ts': client_ts.isoformat()  # Ensure the timestamp is in ISO format
        }

        # Write the record as a JSON object to the file, with each record on a new line
        json.dump(record, f)
        f.write('\n')

print("Sample dataset created in JSONL format with 100,000 rows, including store_error events.")
