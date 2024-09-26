import asyncio
import traceback
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, List, Optional, Union

from pydantic import BaseModel
from rich import print

from indexify.functions_sdk.data_objects import (
    FunctionWorkerOutput,
    IndexifyData,
    RouterOutput,
)
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import IndexifyFunctionWrapper

graphs: Dict[str, Graph] = {}
function_wrapper_map: Dict[str, IndexifyFunctionWrapper] = {}

import concurrent.futures
import io
from contextlib import redirect_stderr, redirect_stdout


class FunctionOutput(BaseModel):
    fn_outputs: Optional[List[IndexifyData]]
    router_output: Optional[RouterOutput]
    reducer: bool = False


class LoggingProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.submit_count = 0

    @staticmethod
    def wrapped_fn(fn, *args, **kwargs):
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        result, exception = [], None
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            try:
                result = fn(*args, **kwargs)
            except Exception:
                exception = traceback.format_exc()

        return result, exception, stdout_capture.getvalue(), stderr_capture.getvalue()

    def submit(self, fn, *args, **kwargs):
        fn_future = super().submit(
            LoggingProcessPoolExecutor.wrapped_fn, fn, *args, **kwargs
        )

        return fn_future


def _load_function(namespace: str, graph_name: str, fn_name: str, code_path: str):
    """Load an extractor to the memory: extractor_wrapper_map."""
    global function_wrapper_map
    key = f"{namespace}/{graph_name}/{fn_name}"
    if key in function_wrapper_map:
        return
    graph = Graph.from_path(code_path)
    function_wrapper = graph.get_function(fn_name)
    function_wrapper_map[key] = function_wrapper
    graph_key = f"{namespace}/{graph_name}"
    graphs[graph_key] = graph


class FunctionWorker:
    def __init__(self, workers: int = 1) -> None:
        self._executor: LoggingProcessPoolExecutor = LoggingProcessPoolExecutor(
            max_workers=workers
        )

    async def async_submit(
        self,
        namespace: str,
        graph_name: str,
        fn_name: str,
        input: IndexifyData,
        code_path: str,
        init_value: Optional[IndexifyData] = None,
    ) -> FunctionWorkerOutput:
        try:
            (
                result,
                exception,
                stdout,
                stderr,
            ) = await asyncio.get_running_loop().run_in_executor(
                self._executor,
                _run_function,
                namespace,
                graph_name,
                fn_name,
                input,
                code_path,
                init_value,
            )
        except BrokenProcessPool as mp:
            self._executor.shutdown(wait=True, cancel_futures=True)
            raise mp

        return FunctionWorkerOutput(
            fn_outputs=result.fn_outputs,
            router_output=result.router_output,
            exception=exception,
            stdout=stdout,
            stderr=stderr,
            reducer=result.reducer,
        )

    def shutdown(self):
        self._executor.shutdown(wait=True, cancel_futures=True)


def _run_function(
    namespace: str,
    graph_name: str,
    fn_name: str,
    input: IndexifyData,
    code_path: str,
    init_value: Optional[IndexifyData] = None,
) -> Union[List[IndexifyData], RouterOutput]:
    print(
        f"[bold] function worker: [/bold] running function: {fn_name} namespace: {namespace} graph: {graph_name}"
    )
    key = f"{namespace}/{graph_name}/{fn_name}"
    if key not in function_wrapper_map:
        _load_function(namespace, graph_name, fn_name, code_path)

    graph: Graph = graphs[f"{namespace}/{graph_name}"]
    if fn_name in graph.routers:
        graph_outputs = graph.invoke_router(fn_name, input)
        return FunctionOutput(
            fn_outputs=None, router_output=graph_outputs, reducer=False
        )

    output = graph.invoke_fn_ser(fn_name, input, init_value)

    is_reducer = graph.get_function(fn_name).indexify_function.accumulate is not None

    return FunctionOutput(fn_outputs=output, router_output=None, reducer=is_reducer)
