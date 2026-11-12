import mlflow
mlflow.set_tracking_uri("http://202.10.42.115:5000")
for e in mlflow.search_experiments():
    print(e.experiment_id, e.name, e.artifact_location)