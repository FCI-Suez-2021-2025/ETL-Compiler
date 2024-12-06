import customtkinter as ctk


from typing import Any, Tuple


class TabButton(ctk.CTkFrame):
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
        title: str = "",
        index: int = 0,
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
        self.title = title
        self.index = index
        self.tab_button: ctk.CTkButton = ...
        self.pack_propagate(False)
        self.add_children_widgets()

    def add_children_widgets(self):
        # Main tab button with left-aligned text
        self.tab_button = ctk.CTkButton(
            self,
            text=self.title,
            font=("Arial", 15),
            fg_color="transparent",
            text_color=("black", "white"),
            hover_color=("#cdcdcd", "#444444"),
            corner_radius=8,
            anchor="w",  # Align text to the left (west)
        )
        self.delete_button = ctk.CTkButton(
            self,
            text="x",
            width=30,
            fg_color=("#ff2b1c", "#cc1616"),
            hover_color="#991010",
        )
        # "..." button
        self.menu_button = ctk.CTkButton(
            self, text="✎", width=30, command=self.open_rename_popup
        )
        self.up_button = ctk.CTkButton(
            self,
            text="↑",
            width=30,
        )

        # Down button
        self.down_button = ctk.CTkButton(
            self,
            text="↓",
            width=30,
        )

        self.tab_button.pack(side="left", fill="both", expand=True, padx=5)
        self.delete_button.pack(side="right", padx=5)
        self.menu_button.pack(side="right", padx=5)
        self.up_button.pack(side="right", padx=5)
        self.down_button.pack(side="right", padx=5)

    def open_rename_popup(self):
        popup = ctk.CTkToplevel(self)
        popup.title("Rename Tab")
        popup.geometry("300x150")
        popup.grab_set()

        entry_label = ctk.CTkLabel(popup, text="Enter new tab name:")
        entry_label.pack(pady=(20, 5))

        title_entry = ctk.CTkEntry(popup)
        title_entry.pack(pady=5)
        title_entry.insert(0, self.title)

        save_button = ctk.CTkButton(
            popup,
            text="Save",
            command=lambda: self.rename_tab(title_entry.get(), popup),
        )
        save_button.pack(pady=10)

    def __truncate_title(self, title: str) -> str:
        max_characters = 13
        return (
            title
            if len(title) <= max_characters
            else title[: max_characters - 3] + "..."
        )

    def rename_tab(self, new_title: str, popup: ctk.CTkToplevel):
        self.title = new_title
        self.tab_button.configure(text=self.__truncate_title(new_title))
        popup.destroy()

    def update_title_and_text(self, new_title: str) -> None:
        self.title = new_title
        self.tab_button.configure(text=self.__truncate_title(new_title))
