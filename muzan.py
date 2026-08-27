# muzan.py — MuzanCI pipeline configuration example.
#
# The primitives Step(), Job(), and Pipeline() are provided by the
# config as Rust StarlarkValue globals.  This file shows how to compose
# them into a real CI workflow.

# ------------------------------------------------------------------------------
# Reusable step helpers (Starlark-level wrappers around the step() primitive).
# ------------------------------------------------------------------------------

def upload(source, destination):
    return Step(
        name="upload {} {}".format(source, destination),
        command="echo aws s3 upload {} {}".format(source, destination),
    )


def download(source, destination):
    return Step(
        name="download {} {}".format(source, destination),
        command="echo aws s3 download {} {}".format(source, destination),
    )


def attest(glob, oidc_issuer):
    command = "echo attest {} {}".format(glob, oidc_issuer)
    return Step(
        name=command,
        command=command,
    )


# ------------------------------------------------------------------------------
# Pipeline definition
# ------------------------------------------------------------------------------

build_job = Job(
    name="build_job",
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    steps=[
        Step(
            name="build",
            command="echo cargo build --release",
        ),
        attest(
            glob="target/release/my_binary",
            oidc_issuer="https://oidc.example.com",
        ),
        upload(
            source="target/release/my_binary",
            destination="s3://my-bucket/{}/my_binary".format(GIT_COMMIT),
        ),
    ],
)

test_job = Job(
    name="test_job",
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    needs=[
        build_job,  # TODO: evaluate to Need(job_id, Completed)
    ],
    steps=[
        download(
            source="s3://my-bucket/{}/my_binary".format(GIT_COMMIT),
            destination="./my_binary",
        ),
        Step(
            name="test",
            command="echo ./my_binary --test --output results.json",
        ),
        attest(
            glob="results.json",
            oidc_issuer="https://oidc.example.com",
        ),
    ],
)

clean_up_job = Job(
    name="clean_up_job",
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    needs=[
        test_job.failed,
    ],
    steps=[
        Step(
            name="clean up",
            command="echo rm -rf target/release/my_binary",
        ),
    ],
)

no_name_job = Job(
    uses=Image(
        ref="alpine:latest",
        arch="amd64",
        os="linux",
    ),
    steps=[],
)

load("external.py", "external_job")

# pipeline() is called for its side effect: it registers the pipeline in the
# config's collector.  It does not need to be assigned to a variable.
for arch in ["x86_64", "arm64"]:
    Pipeline(
        name=f"release_{arch}",
        when=[
            Push(),
        ],
        needs=[test_job, external_job],
    )
