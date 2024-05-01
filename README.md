# Metadata Drive Frameworks

How can I take a parameterized function or script and apply it to many resources at once?

### Where do I keep my resources?
Any time you're defining the definition of what a resource should look like before it gets created, I recommend adding it to a json or yaml file committed in your repo. I'm often asked if it's ok to put these metadata definitions in a table, but a table can be deleted, history can be altered, we don't have a perfect view of what has changed over time. Within git history, we can look back indefinitely at what the intended structure looked like.

For anything that you'd consider "state" - information that was generated AFTER the resource has been created, I would recommend keeping in a table.

Ex. Job definitions are commited as metadata in the repo. When I deploy those job definitions, jobs are created and they're assigned a `job_id`. I would write my repo identifier and the job id to a table so I could map the resource in my repo to the resouce in my workspace. This is my state. The job definition shouldn't change, but let's say I delete and recreate that job, it'll have a new "state", and I'd want to capture that in my specific environment

Futher, I may have definitions for the jobs that will span Dev/Prod environments. The job definitions may match across envs, but the job_ids wouldn't

### Code Examples
There are two demonstrations here:
1. basic dlt framework - showing a simple example how to apply the same function to many tables, but breaking those tables into multiple pipelines
2. basic workflow framework - basically doing the same thing as above, but applying the same notebook to many tables, and breaking those notebook runs into grouped workflows

#### Setup
Before you run any of the code, make sure you run the `_setup` notebook to create the upstream tables

#### basic workflow framework
1. Change the values in `_includes` notebook to match a catalog where you have sufficient privileges to create tables. If possible, use a new schema so you can just drop it at the end
2. Run `jobs_CRUD` to create the job definitions in the `workflows` subdirectory
3. optional, uncommend the code to create a job, and see the output.

#### basic DLT framework
see this [example](https://docs.databricks.com/en/delta-live-tables/create-multiple-tables.html)
1. Run `dlt_CRUD` to create the job definitions in the `workflows` subdirectory
2. optional, uncommend the code to create a job, and see the output.

#### teardown
1. Edit the teardown script to refelct the schema you used for this project.
1. Run the `__teardown script
