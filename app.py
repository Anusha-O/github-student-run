from flask import Flask, request, jsonify
from google.cloud import bigquery
from datetime import datetime
import base64
import json

app = Flask(__name__)
bq_client = bigquery.Client()

# Grade calculation logic
def calc_grade(marks):
    if marks >= 90:
        return "A"
    if marks >= 80:
        return "B"
    if marks >= 70:
        return "C"
    if marks >= 60:
        return "D"
    return "E"

@app.route("/")
def home():
    return "GitHub Cloud Run Service with BigQuery + Grade Calculation"

@app.route("/insert", methods=["POST"])
def insert():
    data = request.get_json()

    name = data.get("name")
    marks = data.get("marks")

    # Calculate grade
    grade = calc_grade(marks)

    # Prepare row for BigQuery
    row = [{
        "name": name,
        "marks": marks,
        "grade": grade,
        "timestamp": datetime.utcnow().isoformat()
    }]

    # Insert to BigQuery
    table = "traffic-07.student_api.grades_table"
    errors = bq_client.insert_rows_json(table, row)

    if errors:
        return jsonify({"status": "error", "errors": errors}), 400

    return jsonify({
        "status": "success",
        "name": name,
        "marks": marks,
        "grade": grade
    })
# -------------- Pub/Sub Trigger Endpoint ----------
@app.route("/pubsub-trigger", methods=["POST"])
def pubsub_trigger():

    envelope = request.get_json()

    if not envelope:
        return ("Bad Request: No Pub/Sub message received", 400)

    if "message" not in envelope:
        return ("Invalid Pub/Sub message format", 400)

    pubsub_message = envelope["message"]

    # Decode Pub/Sub message (base64 encoded)
    if "data" in pubsub_message:
        decoded = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        data = json.loads(decoded)
    else:
        data = {}

    name = data.get("name")
    marks = data.get("marks")

    grade = calc_grade(marks)

    row = [{
        "name": name,
        "marks": marks,
        "grade": grade,
        "timestamp": datetime.utcnow().isoformat()
    }]

    table = "traffic-07.student_api.grades_table"
    errors = bq_client.insert_rows_json(table, row)

    if errors:
        return jsonify({"status": "error", "errors": errors}), 400

    return jsonify({
        "status": "pubsub insert success",
        "name": name,
        "marks": marks,
        "grade": grade
    })
