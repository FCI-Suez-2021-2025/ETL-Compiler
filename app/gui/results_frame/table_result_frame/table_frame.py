from typing import Any, Tuple
import customtkinter as ctk
from pandas import DataFrame

from app.gui.results_frame.table_result_frame.table_widget import TableWidget
from app.gui.visualization import visualize


class TableResultFrame(ctk.CTkFrame):
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
            self,
            text="Table",
        )
        self.table = TableWidget(self)
        # Add "Show Visualization" button
        self.visualize_button = ctk.CTkButton(
            self,
            text="Show Visualization",
            command=lambda: visualize(self, self.get_table()),  # No functionality yet
            width=150,
        )
        # self.data_frame_textbox.configure(state="disabled")  # Make read-only
        self.table_theme = ""
        self.label.pack(pady=5)
        self.table.pack(fill="both", expand=True, padx=5)
        self.table.pack_propagate(False)
        self.visualize_button.pack(pady=5)
        if ctk.get_appearance_mode().lower() == "dark":
            self.change_table_theme("dark")
        else:
            self.table_theme = "light"

    def set_table(self, data_frame: DataFrame) -> None:
        self.table.set_data(data_frame)

    def get_table(self) -> DataFrame:
        return self.table.data

    def clear_table(self) -> None:
        self.table.reset_data()

    def change_table_theme(self, theme: str) -> None:
        self.table_theme = theme
        self.table.sheet.change_theme(theme.lower() + " blue")
