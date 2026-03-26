"""DAV2 base table logic."""


def skip_trx_qa(client):
    return True


def skip_gift_qa(client):
    return True


def force_publish_source_code_despite_errors(client):
    return True
