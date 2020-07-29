
from wolf.ford import prefect, WolfTask, WolfFlow
from prefect.environments.storage.github import GitHub

###### Flow storage ############################
FLOW_STORAGE = GitHub(
    "getzlab/demo_wolf_flow",
    path="example_a.py",
    labels=["example-flow-label"],
    result=prefect.engine.results.LocalResult()
)
################################################

TaskA = WolfTask(
    name = "TaskA",
    overrides = { },
    outputs = {
        "output_a": "*.a"
    },
    script = """
    set -euxo pipefail
    echo ${input_a} > output.a
    """
)

TaskB = WolfTask(
    name = "TaskB",
    overrides = { },
    outputs = {
        "output_b": "*.b"
    },
    script = """
    set -euxo pipefail
    echo ${input_b1} >> output.b
    echo ${input_b2} >> output.b
    """
)

@prefect.task
def readfile_and_prefixstr(path, prefix=""):
    with open(path, "r") as f:
        content = f.read().rstrip()
        content = content + prefix
        return content

@WolfFlow(storage=FLOW_STORAGE)
def DemoFlow(flow_inputA, flow_inputB="default_inputB"):
    ta = TaskA(input_a = flow_inputA)
    tb = TaskB(input_b1 = readfile_and_prefixstr(ta["output_a"], "myprefix_"), input_b2 = flow_inputB)
    return

if __name__ == "__main__":
    DemoFlow.register_getzlab()

