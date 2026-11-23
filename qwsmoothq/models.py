"""消息与统计相关的数据模型。"""

from __future__ import annotations

import time
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl

# 消息生命周期状态
MessageState = Literal["queued", "running", "retrying", "succeeded", "dead"]


class EnqueueRequest(BaseModel):
    """业务方投递请求的入参。"""

    api: str = Field(..., min_length=1, max_length=128, description="接口标识，如 message/send")
    url: HttpUrl = Field(..., description="下游完整请求地址")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict, description="query string 参数")
    body: Any = Field(default=None, description="JSON 请求体")
    max_retries: int = Field(default=3, ge=0, le=20)


class Message(BaseModel):
    """队列中流转的消息体。"""

    id: str
    api: str
    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    body: Any = None
    max_retries: int = 3
    attempt: int = 0
    enqueued_at: float = Field(default_factory=time.time)

    @classmethod
    def from_request(cls, req: EnqueueRequest) -> "Message":
        return cls(
            id=uuid.uuid4().hex,
            api=req.api,
            url=str(req.url),
            method=req.method,
            headers=req.headers,
            params=req.params,
            body=req.body,
            max_retries=req.max_retries,
        )


class EnqueueResponse(BaseModel):
    message_id: str
    state: MessageState


class MessageStatus(BaseModel):
    """单条消息的可查询状态。"""

    id: str
    api: str
    state: MessageState
    attempt: int
    max_retries: int
    enqueued_at: float
    updated_at: float
    last_error: str | None = None
    response_status: int | None = None
    response_body: str | None = None


class DeadLetter(BaseModel):
    """死信队列中的失败消息。"""

    message: Message
    reason: str
    response_status: int | None
    response_body: str | None
    dead_at: float


class StatsPoint(BaseModel):
    """按秒聚合的统计点。"""

    bucket: int  # Unix 秒
    scope: str  # "global" 或具体接口标识
    requested: int = 0
    succeeded: int = 0
    limited: int = 0
    retried: int = 0
    dead: int = 0
    backlog: int = 0
    rate: float | None = None  # 该秒生效的目标速率
