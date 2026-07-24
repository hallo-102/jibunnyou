from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ChatgptPromptGenerateRequest(BaseModel):
    race_id: str = Field(min_length=1, max_length=32)


class ChatgptPromptRead(BaseModel):
    history_id: str
    race_id: str
    prompt_text: str
    prompt_length: int
    warning_threshold: int
    length_warning: bool
    chatgpt_url: str


class ChatgptResponseSaveRequest(BaseModel):
    race_id: str = Field(min_length=1, max_length=32)
    prompt_text: str = Field(min_length=1)
    response_text: str = Field(min_length=1)
    history_id: str | None = Field(default=None, max_length=36)


class ChatgptManualHistoryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    race_id: str
    source: str
    prompt_text: str
    response_text: str | None = None
    created_at: datetime
    updated_at: datetime
