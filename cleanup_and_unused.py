build_job = Job(
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    name="build_job",
    needs=[],
    steps = [],
)

test_job = Job(
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    name="test_job",
    needs=[build_job.completed],
    steps = [],
)

clean_job = Job(
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    name="clean_job",
    needs=[test_job.failed],
    steps = [],
)

unused_job = Job(
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    name="unused_job",
    needs=[],
    steps = [],
)

pipeline = Pipeline(
    name="my_pipeline",
    when = [
        Push(),
    ],
    needs=[
        test_job,
        # unused_job,
    ],
)
