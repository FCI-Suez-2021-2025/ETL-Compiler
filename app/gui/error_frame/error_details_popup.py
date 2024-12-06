from typing import Any, Tuple
import customtkinter as ctk


class ErrorDetailsFrame(ctk.CTkFrame):
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
        error: str = "",
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

        self.error_textbox = ctk.CTkTextbox(self)
        self.error_textbox.insert("1.0", error)
        self.error_textbox.configure(state="disabled")
        self.copy_button = ctk.CTkButton(
            self, text="Copy to Clipboard", command=self.copy_to_clipboard
        )
        self.error_textbox.pack(fill="both", expand=True, padx=5)
        self.copy_button.pack(pady=5)

    def copy_to_clipboard(self):
        # Copy the provided text to the clipboard
        self.clipboard_clear()
        self.clipboard_append(self.error_textbox.get("1.0", "end-1c").strip())
        self.update()


class ErrorDetailsPopUp(ctk.CTkToplevel):
    def __init__(
        self,
        *args,
        fg_color: str | Tuple[str, str] | None = None,
        error: str = "",
        **kwargs
    ):
        super().__init__(*args, fg_color=fg_color, **kwargs)
        self.title("Errors")
        self.geometry("400x250")
        self.grab_set()
        self.error_frame = ErrorDetailsFrame(self, error=error)
        self.error_frame.pack(fill="both", expand=True)
        self.error_frame.pack_propagate(False)
