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
            command="for i in {0..30}; do echo $i; echo $i >&2; sleep 1; done",
        ),
    ],
)
