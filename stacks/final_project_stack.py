import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_iam as iam
import aws_cdk.aws_glue as glue


class FinalProjectAnalysisStack(cdk.Stack):

    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define role for glue to use to have read/write access to final data bucket
        glue_role = iam.Role(self, "my_glue_role",
                             assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                             managed_policies=[
                                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")])
        
        
        # Add glue permission to access AWS Secrets Manager for fetching Ticketmaster API key
        secrets_manager_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["secretsmanager:GetSecretValue"],
            resources=["arn:aws:secretsmanager:us-west-2:943686807189:secret:finalproject/daniel/ticketmaster-Gu2UO4"]
        )

        glue_role.add_to_policy(secrets_manager_policy)

        # Initialize bucket for dumping the data once it is extracted
        data_bucket = s3.Bucket(self, "final_data",
                                versioned=True,
                                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                removal_policy=cdk.RemovalPolicy.DESTROY,
                                auto_delete_objects=True)
        
        # Attach glue policy to our final data bucket
        data_bucket.grant_read_write(glue_role)

        # Create bucket for storing glue scripts
        scripts_bucket = s3.Bucket(self, "glue_scripts",
                            versioned=True,
                            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                            removal_policy=cdk.RemovalPolicy.DESTROY,
                            auto_delete_objects=True)
        
        # Attach glue policy to glue script bucket
        scripts_bucket.grant_read_write(glue_role)
        
        # Dump our scripts into the glue script bucket
        s3deploy.BucketDeployment(self, "deploy",
                                  sources=[s3deploy.Source.asset("./assets/")],
                                  destination_bucket=scripts_bucket,
                                  destination_key_prefix="assets/")
        
        # Create our glue job to download the data
        # First initialize glue workflow
        my_workflow = glue.CfnWorkflow(self, "ticketmaster_workflow",
                                       description="Workflow for processing Ticketmaster data to csv")
        

        # Set up job to run as part of workflow
        ticketmaster_download_job = glue.CfnJob(self, "ticketmaster_download_job",
                                                name="ticketmaster_download_job",
                                                command=glue.CfnJob.JobCommandProperty(
                                                    name="pythonshell",
                                                    python_version="3.9",
                                                    script_location=f"s3://{scripts_bucket.bucket_name}/assets/ticketmaster_to_csv.py"
                                                ),
                                                role=glue_role.role_arn,
                                                glue_version="3.0",
                                                max_capacity=1,
                                                timeout=3,
                                                default_arguments={
                                                    "--my_bucket": data_bucket.bucket_name
                                                })
        
        # Define trigger to run the above job
        start_trigger = glue.CfnTrigger(self, "daniel_trigger",
                                        name="daniel_trigger",
                                        actions=[glue.CfnTrigger.ActionProperty(job_name=ticketmaster_download_job.name)],
                                        type="ON_DEMAND",
                                        workflow_name=my_workflow.name)


        # Potentially set up analysis trigger? Idk if we need anything else for now

        # Configure data catalog
        glue_data_cataloging = glue.CfnDatabase(self, "ticketmaster_db",
                                                catalog_id=cdk.Aws.ACCOUNT_ID,
                                                database_input=glue.CfnDatabase.DatabaseInputProperty(
                                                    name="ticketmaster_db",
                                                    description="Data catalog for ticketmaster data"
                                                ))

        # Configure crawler
        glue_crawler = glue.CfnCrawler(self, "ticketmaster_crawler",
                                       name="ticketmaster_crawler",
                                       role=glue_role.role_arn,
                                       database_name="ticketmaster_db",
                                       targets={"s3Targets": [{"path": f"s3://{data_bucket.bucket_name}/"}]})
        
    



        

