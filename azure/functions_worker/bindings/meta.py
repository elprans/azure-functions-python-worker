import collections.abc
import sys
import typing

from .. import protos
from .. import typing_inspect

from . import generic


class Datum:
    def __init__(self, value, type):
        self.value = value
        self.type = type

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return self.value == other.value and self.type == other.type

    def __hash__(self):
        return hash((type(self), (self.value, self.type)))

    def __repr__(self):
        val_repr = repr(self.value)
        if len(val_repr) > 10:
            val_repr = val_repr[:10] + '...'
        return '<Datum {} {}>'.format(self.type, val_repr)

    def as_proto(self) -> protos.TypedData:
        if self.type == 'string':
            return protos.TypedData(string=self.value)
        elif self.type == 'bytes':
            return protos.TypedData(bytes=self.value)
        elif self.type == 'json':
            return protos.TypedData(json=self.value)
        elif self.type == 'http':
            return protos.TypedData(http=protos.RpcHttp(
                status_code=self.value['status_code'].value,
                headers={k: v.value for k, v in headers.items()},
                enable_content_negotiation=False,
                body=body.as_proto()
            ))
        else:
            raise NotImplementedError(
                'unexpected Datum type: {!r}'.format(self.type)
            )

    @classmethod
    def from_typed_data(cls, td: protos.TypedData):
        tt = td.WhichOneof('data')
        if tt == 'http':
            http = td.http
            val = dict(
                method=Datum(http.method, 'string'),
                url=Datum(http.url, 'string'),
                headers={
                    k: Datum(v, 'string') for k, v in http.headers.items()
                },
                body=Datum.from_typed_data(http.rawBody),
                params={
                    k: Datum(v, 'string') for k, v in http.params.items()
                },
                query={
                    k: Datum(v, 'string') for k, v in http.query.items()
                },
            )
        elif tt == 'string':
            val = td.string
        elif tt == 'bytes':
            val = td.bytes
        elif tt == 'json':
            val = td.json
        else:
            raise NotImplementedError(
                'unsupported TypeData kind: {!r}'.format(tt)
            )

        return cls(val, tt)


def get_binding_registry():
    func = sys.modules.get('azure.functions')
    if func is not None:
        return func.get_registry()
    else:
        return None


def is_iterable_type_annotation(annotation: object, pytype: object) -> bool:
    is_iterable_anno = (
        typing_inspect.is_generic_type(annotation) and
        issubclass(typing_inspect.get_origin(annotation),
                   collections.abc.Iterable)
    )

    if not is_iterable_anno:
        return False

    args = typing_inspect.get_args(annotation)
    if not args:
        return False

    if isinstance(pytype, tuple):
        return any(isinstance(t, type) and issubclass(t, arg)
                   for t in pytype for arg in args)
    else:
        return any(isinstance(pytype, type) and issubclass(pytype, arg)
                   for arg in args)


def get_binding(bind_name: str) -> object:
    binding = None
    registry = get_binding_registry()
    if registry is not None:
        binding = registry.get(bind_name)

    if binding is None:
        binding = generic.GenericBinding

    return binding


def is_trigger_binding(bind_name: str) -> bool:
    binding = get_binding(bind_name)
    return binding.has_trigger_support()


def check_input_type_annotation(binding: str, pytype: type,
                                datatype: protos.BindingInfo.DataType) -> bool:
    binding = get_binding(binding)
    return binding.check_input_type_annotation(pytype, datatype)


def check_output_type_annotation(binding: str, pytype: type) -> bool:
    binding = get_binding(binding)
    return binding.check_output_type_annotation(pytype)


def from_incoming_proto(
        binding: str,
        val: protos.TypedData, *,
        pytype: typing.Optional[type],
        trigger_metadata: typing.Optional[typing.Dict[str, protos.TypedData]])\
        -> typing.Any:

    binding = get_binding(binding)
    datum = Datum.from_typed_data(val)
    metadata = {
        k: Datum.from_typed_data(v) for k, v in trigger_metadata.items()
    }

    try:
        return binding.decode(datum, trigger_metadata=metadata)
    except NotImplementedError:
        # Binding does not support the data.
        dt = val.WhichOneof('data')

        raise TypeError(
            f'unable to decode incoming TypedData: '
            f'unsupported combination of TypedData field {dt!r} '
            f'and expected binding type {binding}')


def to_outgoing_proto(binding: str, obj: typing.Any, *,
                      pytype: typing.Optional[type]) -> protos.TypedData:
    binding = get_binding(binding)

    try:
        datum = binding.encode(obj, expected_type=pytype)
    except NotImplementedError:
        # Binding does not support the data.
        raise TypeError(
            f'unable to encode outgoing TypedData: '
            f'unsupported type "{binding}" for '
            f'Python type "{type(obj).__name__}"')

    return datum.as_proto()
