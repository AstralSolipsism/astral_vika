# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# Basic Property Models
class EmptyProperty(BaseModel):
    """For fields without properties, such as Text, Attachment, etc."""
    pass


class DefaultValueProperty(BaseModel):
    defaultValue: Optional[str] = None


# Select Properties
class Color(BaseModel):
    name: str
    value: str


class SelectOption(BaseModel):
    id: str
    name: str
    color: Color


class SelectProperty(BaseModel):
    options: List[SelectOption]


# Number Properties
class NumberProperty(BaseModel):
    defaultValue: Optional[str] = None
    precision: int
    commaStyle: Optional[str] = None
    symbol: Optional[str] = None


class CurrencyProperty(BaseModel):
    defaultValue: Optional[str] = None
    precision: int
    symbol: str
    symbolAlign: Optional[str] = "Default"


class PercentProperty(BaseModel):
    defaultValue: Optional[str] = None
    precision: int


# DateTime Properties
class DateTimeProperty(BaseModel):
    dateFormat: str
    includeTime: bool
    timeFormat: Optional[str] = None
    autoFill: bool
    timeZone: Optional[str] = None
    includeTimeZone: Optional[bool] = False


# Member/User Properties
class MemberProperty(BaseModel):
    isMulti: bool
    shouldSendMsg: bool


class UserOption(BaseModel):
    id: str
    name: str
    avatar: str


class UserProperty(BaseModel):
    options: List[UserOption]


# Other Simple Properties
class CheckboxProperty(BaseModel):
    icon: str


class RatingProperty(BaseModel):
    icon: str
    max: int


# Link Properties
class LinkProperty(BaseModel):
    foreignDatasheetId: str
    limitToViewId: Optional[str] = None
    limitSingleRecord: Optional[bool] = False


class TwoWayLinkProperty(LinkProperty):
    brotherFieldId: str


# Formula Property
class FormulaFormat(BaseModel):
    type: str
    # Specific format properties depend on the type
    dateFormat: Optional[str] = None
    timeFormat: Optional[str] = None
    includeTime: Optional[bool] = None
    precision: Optional[int] = None
    symbol: Optional[str] = None


class FormulaProperty(BaseModel):
    expression: str
    valueType: str
    hasError: Optional[bool] = False
    format: Optional[FormulaFormat] = None


# Button Property
class ButtonStyle(BaseModel):
    color: Optional[str] = None
    fill: Optional[bool] = None
    bold: Optional[bool] = None


class ButtonAction(BaseModel):
    type: str
    # Action properties depend on the type
    url: Optional[str] = None
    openInNewTab: Optional[bool] = None
    datasheetId: Optional[str] = None
    viewId: Optional[str] = None
    fieldId: Optional[str] = None
    recordId: Optional[str] = None
    automationId: Optional[str] = None


class ButtonProperty(BaseModel):
    text: str
    style: Optional[ButtonStyle] = None
    action: Optional[ButtonAction] = None