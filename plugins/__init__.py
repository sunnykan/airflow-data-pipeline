from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.HasRowsOperator,
        operators.S3ToRedshiftOperator,
        operators.LoadFactOperator,
    ]
    helpers = [helpers.SqlQueries]

