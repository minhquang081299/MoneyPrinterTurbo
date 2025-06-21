class BaseStorage:
    def save(self, data: dict) -> None:
        """
        Save data to the storage.
        
        :param data: Data to be saved
        :type data: dict
        :return: None
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def retrieve(self, identifier: str) -> dict:
        """
        Retrieve data from the storage using an identifier.
        
        :param identifier: Identifier for the data to be retrieved
        :type identifier: str
        :return: Retrieved data
        """
        raise NotImplementedError("This method should be overridden by subclasses.")