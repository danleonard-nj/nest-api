

class Queryable:
    def get_query(
        self
    ) -> dict:
        raise NotImplementedError

    def get_pipeline(
        self
    ) -> dict:
        raise NotImplementedError

    def get_sort(
        self
    ) -> list:
        raise NotImplementedError
