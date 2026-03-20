# Copyright (c) Simon Niederberger.
# Distributed under the terms of the MIT License.

from .hook import PipelineSystemTrace

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

__all__ = ["PipelineSystemTrace"]
