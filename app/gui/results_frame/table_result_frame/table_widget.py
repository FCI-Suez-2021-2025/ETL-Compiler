from typing import Any, Tuple
from tksheet import Sheet
from pandas import DataFrame
import customtkinter as ctk


class TableWidget(ctk.CTkFrame):
    def __init__(
        self,
        master: Any,
        width: int = 200,
        height: int = 200,
        corner_radius: int | str | None = None,
        border_width: int | str | None = None,
        bg_color: str | Tuple[str, str] = "transparent",
        fg_color: str | Tuple[str, str] | None = None,
        border_color: str | Tuple[str, str] | None = None,
        background_corner_colors: Tuple[str | Tuple[str, str]] | None = None,
        overwrite_preferred_drawing_method: str | None = None,
        **kwargs
    ):
        super().__init__(
            master,
            width,
            height,
            corner_radius,
            border_width,
            bg_color,
            fg_color,
            border_color,
            background_corner_colors,
            overwrite_preferred_drawing_method,
            **kwargs
        )
        self.data = DataFrame()
        self.sheet = Sheet(self)
        self.sheet.enable_bindings("all")
        self.sheet.pack(fill="both", expand=True)

    def set_data(self, data: DataFrame) -> None:
        self.reset_data()
        self.data = data
        self.sheet.set_data(data=self.data.values.tolist())
        self.sheet.set_header_data(list(self.data.columns))

    def get_data(self) -> DataFrame:
        return self.data

    def reset_data(self) -> None:
        self.data = DataFrame()
        self.sheet.reset()
