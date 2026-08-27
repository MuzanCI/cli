external_job = Job(
    name="external_job",
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    steps=[
        Step(
            name="external_step",
            command="echo 'Hello from external job!'",
        ),
    ],
)
