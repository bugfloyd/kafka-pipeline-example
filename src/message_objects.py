class UserQuoteKey(object):
    """
    UserQuoteKey record

    Args:
        user_id (int): User ID

    """
    def __init__(self, user_id):
        self.user_id = user_id


class UserQuoteValue(object):
    """
    UserQuoteValue record

    Args:
        product_id (int): User's name

        quoted_price (int): User's quoted price

        quoted_quantity (int): User's quoted quantity

        user_note (string): User's note

    """
    def __init__(self, product_id, quoted_price, quoted_quantity, user_note):
        self.product_id = product_id
        self.quoted_price = quoted_price
        self.quoted_quantity = quoted_quantity
        self.user_note = user_note
