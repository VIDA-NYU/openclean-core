# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from pdpipe import PdPipelineStage


class GroupOp(PdPipelineStage):
    def __init__(self, exmsg=None, appmsg=None, desc=None):
        super(GroupOp, self).__init__(exmsg=exmsg, appmsg=appmsg, desc=desc)
