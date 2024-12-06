from typing import Any, Tuple
import customtkinter as ctk

from app.gui.error_frame.error_details_popup import ErrorDetailsPopUp


class ErrorFrame(ctk.CTkFrame):
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
        **kwargs,
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
            **kwargs,
        )
        self.error = "Errors:There is no errors"
        self.label = ctk.CTkLabel(self, text=self.error, anchor="w", justify="left")
        self.details_button = ctk.CTkButton(
            self, text="Details", command=self.open_error_details
        )

        self.label.pack(side="left", fill="both", expand=True, padx=10, pady=5)
        self.details_button.pack(side="right", padx=10, pady=5)

    def clear_error(self) -> None:
        self.error = "Errors:There is no errors"
        self.label.configure(text=self.error)

    def set_error(self, error: str) -> None:
        self.error = error

        if len(error_lines := error.split("\n")) > 2:
            self.label.configure(text="\n".join(error_lines[:2]) + "...")
        else:
            self.label.configure(text=self.error)

    def open_error_details(self) -> None:
        ErrorDetailsPopUp(self, error=self.error)
