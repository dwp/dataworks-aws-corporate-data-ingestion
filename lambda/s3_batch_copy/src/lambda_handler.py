from copy import deepcopy


def process_task(task_json):
    print(f"TASK_JSON: {task_json}")
    return f"task_id: {task_json['taskId']} processed"


def handler(event, _context):
    print(f"EVENT: {event}")
    invocation_id = event["invocationId"]

    event_response = {
        "invocationSchemaVersion": "1.0",
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": invocation_id,
        "results": None
    }

    results = []

    for task in event["tasks"]:
        task_id = task["taskId"]

        _task_response_template = {
            "taskId": task_id,
            "resultCode": "Succeeded|TemporaryFailure|PermanentFailure",
            "resultString": "Comment for completion report",
        }

        task_response = deepcopy(_task_response_template)

        try:
            result = process_task(task)
            task_response["resultCode"] = "Succeeded"
            task_response["resultString"] = result
        except Exception as e:
            task_response["resultCode"] = "PermanentFailure"
            task_response["resultString"] = str(e)

        results.append(task_response)

    event_response["results"] = results
    print(f"EVENT_RESPONSE: {event_response}")
    return event_response

