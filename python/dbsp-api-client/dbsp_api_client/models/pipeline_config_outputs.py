from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.output_endpoint_config import OutputEndpointConfig


T = TypeVar("T", bound="PipelineConfigOutputs")


@define
class PipelineConfigOutputs:
    """Output endpoint configuration."""

    additional_properties: Dict[str, "OutputEndpointConfig"] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        pass

        field_dict: Dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        field_dict.update({})

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.output_endpoint_config import OutputEndpointConfig

        d = src_dict.copy()
        pipeline_config_outputs = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = OutputEndpointConfig.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        pipeline_config_outputs.additional_properties = additional_properties
        return pipeline_config_outputs

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> "OutputEndpointConfig":
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: "OutputEndpointConfig") -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
