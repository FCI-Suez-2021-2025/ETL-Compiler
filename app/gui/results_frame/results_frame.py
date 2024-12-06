from typing import Any, Tuple
import customtkinter as ctk

from app.gui.results_frame.python_frame import PythonResultFrame
from app.gui.results_frame.table_result_frame.table_frame import TableResultFrame


class ResultsFrame(ctk.CTkFrame):
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
        self.python_section = PythonResultFrame(self)
        self.table_section = TableResultFrame(self)
        self.python_section.pack(side="left", fill="both", expand=True, padx=5)
        self.python_section.pack_propagate(False)
        self.table_section.pack(side="right", fill="both", expand=True, padx=5)
