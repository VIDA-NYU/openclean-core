
import pandas as pd

from openclean.function.eval.base import Col
from openclean.profiling.count import counts, Count


def test_profile_predicate_counts():
    """Test computing counts for evaluation of predicates on a given data
    frame.
    """
    R1 = [1, 2, 3]
    R2 = [3, 4, 5]
    df = pd.DataFrame(data=[R1, R2, R1, R2, R1], columns=['A', 'B', 'C'])
    counters = {'R1': Count(Col('A') == 1), 'R2': Count(Col('B') > 3)}
    result = counts(df, counters)
    assert result == {'R1': 3, 'R2': 2}
    # Invert the truth values
    counters = {
        'R1': Count(Col('A') == 1, False),
        'R2': Count(Col('B') > 3, False)
    }
    result = counts(df, counters)
    assert result == {'R1': 2, 'R2': 3}
