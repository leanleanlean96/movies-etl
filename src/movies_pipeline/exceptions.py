class MaxPagesBudgetExceeded(Exception):
    pass


class QueryPageLimitExceeded(MaxPagesBudgetExceeded):
    pass


class PartitionPlanError(Exception):
    pass
