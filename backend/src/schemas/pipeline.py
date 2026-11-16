from pydantic import BaseModel, Field


class StepInput(BaseModel):
    key: str = Field(min_length=1, max_length=40)
    kind: str = Field(max_length=30)
    name: str = Field(default='', max_length=160)
    parents: list[str] = Field(default_factory=list, max_length=8)
    params: dict = Field(default_factory=dict)
    is_exit: bool = False


class DefinitionInput(BaseModel):
    steps: list[StepInput] = Field(min_length=1, max_length=40)


class FlowInput(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    definition: DefinitionInput
    note: str = Field(default='', max_length=400)


class RevisionInput(BaseModel):
    definition: DefinitionInput
    note: str = Field(default='', max_length=400)
