from openclean.op.group.base import GroupOp


class FDViolation(GroupOp):
    def __init__(self, lhs, rhs):
        super(FDViolation, self).__init__(exmsg=None, appmsg=None, desc=None)
        self.lhs = lhs
        self.rhs = rhs

    def _prec(self, df):
        return True

    def _transform(self, df, verbose):
        # Group dataframe by columns in the left-hand-side of the FD.
        groups = df.groupby(self.lhs)
        # Keep groups that have more than one distinct value for the attributes
        # of the right-hand-size of the FD.
        fgroups = groups.filter(lambda g: g[self.rhs].nunique() > 1)
        return fgroups.groupby(self.lhs)
