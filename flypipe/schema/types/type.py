class Type:

    NAME = None
    ALLOWED_TYPE_CASTS = []

    def validate(self, df, column_name):
        pass

    @classmethod
    def raise_validation_error(cls, column_name, column_type):
        raise ValueError(
            f'Dataframe type for column "{column_name}" is "{column_type}" which is not compatible with schema type {cls.NAME}'
        )

    def cast(self, df, column_name, destination_type):
        if self.NAME == destination_type.NAME:
            return df
        elif destination_type.NAME not in self.ALLOWED_TYPE_CASTS:
            raise TypeError(
                f'Unable to cast column "{column_name}" to {destination_type.NAME}'
            )
        conversion_func = getattr(self, f"cast_{destination_type.NAME}", None)
        if not conversion_func:
            raise Exception(
                f"Conversion function from {self} to {destination_type.NAME} not found"
            )
        return conversion_func(df, column_name, **destination_type.extra_data)
