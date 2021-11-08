from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    # copy_sql = """
    #     COPY {}
    #     FROM '{}'
    #     ACCESS_KEY_ID '{}'
    #     SECRET_ACCESS_KEY '{}'
    #     REGION AS '{}'
    #     FORMAT AS json '{}'
    # """
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        region="",
        filetype_params={"filetype": "json", "format": "auto"},
        # format="auto",
        *args,
        **kwargs,
    ):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.filetype_params = filetype_params
        # self.format = format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        print(rendered_key)

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        self.log.info(f"Loading staging table: {self.table}")
        base_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
        )

        if self.filetype_params["filetype"].lower() == "json":
            formatted_sql = base_sql + "FORMAT AS json'{}'".format(
                self.filetype_params["format"]
            )
        elif self.filetype_params["filetype"].lower() == "csv":
            formatted_sql = base_sql + "IGNOREHEADER {} DELIMITER '{}'".format(
                self.filetype_params["ignoreheader"], self.filetype_params["delimiter"]
            )
        redshift.run(formatted_sql)
