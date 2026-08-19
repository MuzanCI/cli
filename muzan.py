job1 = Job(
    name = "job1",
    uses = Image(
        ref = "scratch",
        arch = "arm64",
        os = "freebsd"
    ),
    steps = [
        Step(
            name = "step1",
            command = "sleep 100"
        ),
        Step(
            name = "step2",
            command = "echo 'step2'"
        ),
        Step(
            name = "step3",
            command = "echo 'step3'"
        ),
        Step(
            name = "step4",
            command = "echo 'step4'"
        ),
    ]
)
