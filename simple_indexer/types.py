import typing


class RestorationFilterRequest(typing.TypedDict):
    filterKeys: typing.List[str]


class RestorationFilterResponse(typing.TypedDict):
    pushDataHashHex: str
    transactionId: str
    index: int
    referenceType: int
    spendTransactionId: typing.Optional[str]
    spendInputIndex: int
