from typing import Any, Tuple
import customtkinter as ctk


class PythonResultFrame(ctk.CTkFrame):
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
        self.label = ctk.CTkLabel(
            self, text="Python Code"  # , font=("Arial", 12, "bold")
        )
        self.code_textbox = ctk.CTkTextbox(self)
        self.copy_button = ctk.CTkButton(
            self, text="Copy to Clipboard", command=self.copy_to_clipboard
        )
        self.label.pack(pady=5)
        self.code_textbox.pack(fill="both", expand=True, padx=5)
        self.copy_button.pack(pady=5)

    def set_code(self, python_code: str) -> None:
        self.code_textbox.delete("1.0", "end")
        self.code_textbox.insert("1.0", python_code)

    def get_code(self) -> str:
        return self.code_textbox.get("1.0", "end-1c").strip()

    def clear_code(self) -> None:
        self.code_textbox.delete("1.0", "end")

    def copy_to_clipboard(self):
        # Copy the provided text to the clipboard
        self.clipboard_clear()
        self.clipboard_append(self.code_textbox.get("1.0", "end-1c").strip())
        self.update()
