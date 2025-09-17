# idfy_assignment
This repository is just to submit IDfy assignment

# My Initial Thoughs
1. events_fact is supposed to be the only source_of_truth
2. 


## handling given error cases 

1) While submitting a question or test, the store call fails due to some system issue.
    - Implementing retry logic for the call on app side
    - App should emit event before or in parallel to writing to db
    - In anyway we should also recieve the failed event message in the PubSub so i have included store_error as one of the event type.
        - It could also be a simple error event with error_payload message stating "db_store_failed" or "client_error" which can be pushed to DLQ for failed event messages


2) There is a bug in your app due to which students can submit the answers to any question or
submit the test even 15 minutes after the session has expired.
    - Using is_late_submission_flag
        - If there's session_start_time in the payload this can be calculated on the go in apache_beam. 
        - If not available
            - can be checked on the client-side and sent in payload.
            - can be indentified using query on the data in warehouse

# event_payload from application
{
    event_id STRING,
    event_type STRING, [session_start,session_end,page_view,answer_submit,answer_resubmit,test_submit,store_error]
    test_id STRING,
    student_id STRING,
    question_id STRING,
    session_id STRING,
    answer STRING NULLABLE,
    is_correct_answer INT/BOOL NULLABLE,
    attempt_no INT,
    client_ts TIMESTAMP
}

