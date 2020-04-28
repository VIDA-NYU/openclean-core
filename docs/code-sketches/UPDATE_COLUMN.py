"""Example API methods and classes for updating the values in a data frame
column (discussed 04/26/2018).

An UPDATE_COLUMN function is part of the cleaning library. The function takes a
data frame, column identifier and value transformer function. The value
transformer is called for each column value to produce a transformed (updated)
value.

Alternatively, we could model this as a method of a data frame column, e.g.,
data_frame.columns(col_id).update(value_transformer)

One open question is whether we want the update to create a modified copy of the
data frame or update the data frame 'in place'.

Note that all class, function, and variable names are simple suggestions and
have not been discussed yet. Also, the code below is pseudo code.
"""

# ------------------------------------------------------------------------------
# Basic Library Components
# ------------------------------------------------------------------------------

def UPDATE_COLUMN(data_frame, col_id, value_transformer):
    """Create a modified copy of a given data frame. In the result, all values
    in the column identified by col_id will have been updated using the given
    value transformer.

    Parameters
    ----------
    data_frame: Data frame
    col_id: Unique identifier for column in data_frame
    value_transformer: Function that thakes a scalar value and annotations and
        returns a potentially modified value and annotations
    """
    # First make a copy of the given data frame. Then update all values in the
    # column identified by col_id
    result = df.copy()
    for row in data_frame.rows():
        # Get current cell value and annotations
        value = row.value(col_id)
        annotations = data_frame.metadata.for_cell(col_id, row.id)
        # Call value transformer to get modified value and annotations
        upd_val, upd_anno = value_transformer(value, annotations)
        # Set the modified value
        result.set_value(col_id, row_id, upd_val)
        # Update annotations for modified value
        result.for_cell(col_id, row.id).update(upd_anno)
    return result


class ValueTransformer(object):
    """Abstract template for value transformer. Sets the optional fallback
    handler. The fallback handler is intended to be used by a value transformer
    for those values that the transfomer cannot process.
    """
    def __init__(self, fallback_handler=None):
        """Initialize optional fallback handler.

        Parameters
        ----------
        fallback_handler: func, optional
            Value transformer function that takes scalar value and value
            annotations as arguments.
        """
        self.fallback_handler = fallback_handler

    @abstractmethod
    def __call__(self, value, annotations):
        """Interface for value transformer function. Takes a scalar cell value
        and the cell annotations as input. Is expected to return a scalar value
        and an 'annotation update statement' as output.

        The function may raise ValueTransformError if it cannot transform a
        given value.

        In it's simplest form an 'annotation update statement' is None which
        indicates that annotations have not been changed. An empty dictionary
        indicates that all annotations for the cell have been reset. To update
        individual annotations return a dictionary of (key,value) pairs. A pair
        (key, None) in that dictionary singnals that the annotation with key
        is deleted. If value is not None an existing annotation with key is
        updated or a new annotation inserted (UPSERT).

        Parameters
        ----------
        value: scalar value
        annotations: ObjectAnnotations (e.g., a dictionary?)
        """
        raise NotImplementedError

    def fallback(self, value, annotations):
        """Call fallback handler for values that this transformer implementation
        cannot transform.Raises a ValueTransformError if not fallback handler
        was given at class instantiation.

        Parameters
        ----------
        value: scalar value
        annotations: ObjectAnnotations (e.g., a dictionary?)
        """
        if not self.fallback_handler is None:
            return self.fallback_handler(value, annotations)
        else:
            raise ValueTransformError(value)


# ------------------------------------------------------------------------------
# Example ValueTransformer for 'Standardize Categorical Values' use case.
#
# The task is to standardize the values in a column such that all values are
# from a (partially) given domain of categorical values. For example, vehicle
# color in NYC Open Data Parkin Violations.
# ------------------------------------------------------------------------------


class AskUser(ValueTransformer):
    """Some form of getting user input during update execution."""
    def __init__(self, message_template, fallback_handler=None):
        """Domain can be a set or list (or anything that implements the
        __contains__ method). Defines the set of values in a domain.
        """
        super(AskUser, self).__init__(fallback_handler=fallback_handler)
        self.message_template = message_template

    def __call__(self, value, annotations):
        """Ask user for input
        """
        user_val = raw_input(self.message_template(value))
        if not user_val is None:
            # Maybe maintain information about inputs to avoid asking user again
            # for same value (e.g., we could modify a mapping dictionary here)
            return user_val, annotations
        else:
            return self.fallback(value, annotations)


class Domain(ValueTransformer):
    """Domain checks if a value is an element of a given domain."""
    def __init__(self, domain, fallback_handler=None):
        """Domain can be a set or list (or anything that implements the
        __contains__ method). Defines the set of values in a domain.
        """
        super(Domain, self).__init__(fallback_handler=fallback_handler)
        self.domain = domain

    def __call__(self, value, annotations):
        """Return value if it is in the domain. Otherwise, call fallback
        handler.
        """
        if value in self.domain:
            return value, annotations
        else:
            return self.fallback(value, annotations)


class Translate(ValueTransformer):
    def __init__(self, mapping, fallback_handler=None):
        """Translate values based on a given mapping function (dictionary)."""
        super(Translate, self).__init__(fallback_handler=fallback_handler)
        self.mapping = mapping

    def __call__(self, value, annotations):
        """Return the result of mapping(value) or call fallback handler if not
        mapping  is defined for value.
        """
        if value in self.mapping:
            return self.mapping[value], annotations
        else:
            return self.fallback(value, annotations)


class ValueCollector(object):
    def __init__(self):
        """Maintain a set of all distinct values that were passed to this
        value transformer. Used to collect the set of values for which no
        mapping in a translator's mapping function were defined.
        """
        self.values = set()

    def __call__(self, value, annotations):
        """Add the given value to the internal set of distinct values and
        return value and annotations without modifications.
        """
        if not value in self.values:
            self.values.add(value)
        return value, annotations




# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

# Create data frame for 'dirty data set'. The values in column vehicle_color are
# for example RED, GREEN, WHITE, BLACK, RD, GN, WH, BR
df = DataFrame().load('NYCOpenData_ParkingViolations')
# Define set of valid/known values for column vehicle_color. We discussed that
# this set does not have to be complete initially.
colors = set({'RED', 'GREEN', 'WHITE', 'BLACK'})
# Define mapping from RD -> RED and GN -> GREEN
mapping = {'RD': 'RED', 'GN': 'GREEN'}
# To standardize the values in column vehicle_color we (1) check whether a given
# value is in the domain of colors (no changes needed), (2) check whether there
# is a mapping for a value to a known value in the domain (translate), and
# (3) collect all those unknown values that are not in the domain and for which
# no mapping is defined
done = False
while not done:
    # Set-up the translation chain (needs to be done in reverse order of steps
    # descriped above).
    unknown_values = ValueCollector()
    translator = Translate(mapping, fallback_handler=unknown_values)
    domain = Domain(colors, fallback_handler=translator)
    # Run update
    df = UPDATE_COLUMN(df, 'vehicle_color', domain)
    # Check if there were any values in the data frame that could not be
    # translated
    if len(unknown_values.values) > 0:
        # Need to extend domain or mapping to include unknown values. In our
        # example unknown_values.values contains 'WH' and 'BR' after the first
        # run. The following could be the result of some interaction with the
        # user
        colors.add('BROWN')
        mapping['BR'] = 'BROWN'
        mapping['WH'] = 'WHITE'
    else:
        done = True


# The following is an alternative that add a AskUser function in between the
# second and third step to let user define mapping for unknown values during
# update execution
    unknown_values = ValueCollector()
    user_io = AskUser('Privide mapping for %v', fallback_handler=unknown_values)
    translator = Translate(mapping, fallback_handler=user_io)
    domain = Domain(colors, fallback_handler=translator)
