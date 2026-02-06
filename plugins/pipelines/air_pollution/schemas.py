import logging

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)


class _AqiQualityComponents(BaseModel):
    """
    Pydantic model for air quality components data.

    Represents pollutant concentrations from air quality monitoring API.
    All values are validated to ensure non-negative measurements.
    """

    co: float = Field(..., ge=0, description="Carbon monoxide")
    no: float = Field(..., ge=0, description="Nitrogen monoxide")
    no2: float = Field(..., ge=0, description="Nitrogen dioxide")
    o3: float = Field(..., ge=0, description="Ozone")
    so2: float = Field(..., ge=0, description="Sulphur dioxide")
    pm2_5: float = Field(..., ge=0, description="Particulates PM2.5")
    pm10: float = Field(..., ge=0, description="Particulates PM10")
    nh3: float = Field(..., ge=0, description="Ammonia")

    @field_validator("*")
    @classmethod
    def check_non_negative(cls, v, info):
        """Validate that component values are non-negative."""
        if v < 0:
            logger.error(
                f"Validation failed for field '{info.field_name}': negative value {v} is not allowed"
            )
            raise ValueError(f"Value for {info.field_name} cannot be negative.")
        logger.debug(f"Field '{info.field_name}' validated successfully with value: {v}")
        return v


class _MainInfo(BaseModel):
    """
    Pydantic model for main air quality index information.

    Contains the overall AQI rating on a scale from 1 (good) to 5 (very poor).
    """

    aqi: int = Field(..., ge=1, le=5, description="Air Quality Index (1-5)")


class AirPollutionRecord(BaseModel):
    """
    Data class representing a complete air pollution record.

    Aggregates timestamp, main AQI information, and detailed pollutant components
    from air quality monitoring measurements.
    """

    model_config = ConfigDict(strict=False, extra="ignore", frozen=True)

    dt: int = Field(..., ge=946681200, le=2524604400, description="Timestamp")
    main: _MainInfo
    components: _AqiQualityComponents
