import boto3
import fsspec
import loguru

S3_ROOT_PATH = "s3://tngglue/python_modules"
if __name__ == "__main__":
    cwd = fsspec.get_mapper(".")
    s3_path = fsspec.get_mapper(S3_ROOT_PATH)
    modules = []
    for k in cwd:
        if k.endswith(".py"):
            s3_path[k] = cwd[k]
            if k != "glue_driver.py":
                modules.append(f"{S3_ROOT_PATH}/{k}")

    client = boto3.client("glue", region_name="us-east-1")
    JOB_NAME = "test_cc_job4"
    client.delete_job(JobName=JOB_NAME)
    job = client.create_job(
        Name=JOB_NAME,
        Command={
            "Name": "glueetl",
            "ScriptLocation": f"{S3_ROOT_PATH}/glue_driver.py",
            "PythonVersion": "3",
        },
        DefaultArguments={'--additional-python-modules': "fastwarc==0.13.5,loguru==0.6.0,pysimdjson==5.0.2",
                          "--extra-py-files": ",".join(modules),
                          "--enable-continuous-cloudwatch-log": "true",
                          "--enable-continuous-log-filter": "true", 
                          "--enable-spark-ui": 'true',
                          '--spark-event-logs-path': 's3://tngglue/sparklogs/'},
        Role="arn:aws:iam::753985720788:role/GlueAdminRole",
        GlueVersion="3.0",
    )
    print(job)
    res = client.start_job_run(JobName=JOB_NAME)
    print(res)
