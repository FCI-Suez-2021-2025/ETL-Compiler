from typing import Any, Tuple
import customtkinter as ctk

from app.gui.vertical_tab_view.content_frame import TabContent
from app.gui.vertical_tab_view.tab_button import TabButton


# Initialize customtkinter
ctk.set_appearance_mode("dark")  # Modes: "System" (default), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Themes: "blue" (default), "green", "dark-blue"


class VerticalTabView(ctk.CTkFrame):
    def __init__(
        self,
        master: Any,
        width: int = 200,
        height: int = 200,
        corner_radius: int | str | None = None,
        border_width: int | str | None = None,
        bg_color: str | Tuple[str] = "transparent",
        fg_color: str | Tuple[str] | None = None,
        border_color: str | Tuple[str] | None = None,
        background_corner_colors: Tuple[str | Tuple[str]] | None = None,
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

        self.tabs_buttons = list[TabButton]()
        self.tabs_contents = list[TabContent]()

        # Scrollable frame for tabs buttons
        self.tabs_buttons_frame = ctk.CTkScrollableFrame(self, width=350, height=350)
        self.tabs_buttons_frame.pack(side="left", fill="y", padx=10, pady=10)

        # Create a frame to display the activated tab's content next to the tab buttons
        self.content_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.content_frame.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        self.current_tab_index = -1
        # Create the first tab by default
        self.add_tab()
        self.show_tab(0)

    def update_indices(self):
        for i, button in enumerate(self.tabs_buttons, start=0):
            button.index = i

    def delete_tab(self, index: int):
        is_the_current_tab = self.current_tab_index == index
        button, content = self.tabs_buttons.pop(index), self.tabs_contents.pop(index)
        button.destroy()
        content.destroy()
        self.update_indices()
        tabs_length = len(self.tabs_buttons)
        if self.current_tab_index > index:
            self.current_tab_index -= 1
        elif tabs_length > 0 and is_the_current_tab:
            self.current_tab_index = -1
            if index == 0:
                self.show_tab(0)
            elif index == tabs_length:
                self.show_tab(tabs_length - 1)
            else:
                self.show_tab(index)
        elif tabs_length == 0:
            self.current_tab_index = -1

    def add_tab(self, title: str = "New Query Tab"):
        new_tab_button = TabButton(
            self.tabs_buttons_frame,
            height=50,
            corner_radius=8,
            title=title,
            index=len(self.tabs_buttons),
        )
        new_tab_button.tab_button.configure(
            command=lambda: self.show_tab(new_tab_button.index)
        )
        new_tab_button.delete_button.configure(
            command=lambda: self.delete_tab(new_tab_button.index)
        )
        new_tab_button.up_button.configure(
            command=lambda: self.move_tab(new_tab_button.index, -1)
        )
        new_tab_button.down_button.configure(
            command=lambda: self.move_tab(new_tab_button.index, 1)
        )
        colors = ["green", "black", "yellow", "red"]
        new_content_frame = TabContent(
            self.content_frame,
            # fg_color=colors[len(self.tabs_buttons) % len(colors)],
            fg_color="transparent",
        )
        new_tab_button.pack(fill="x", pady=5, padx=5)
        new_tab_button.pack_propagate(False)  # Prevent resizing
        self.tabs_buttons.append(new_tab_button)
        self.tabs_contents.append(new_content_frame)

    def show_tab(self, index: int):
        if index == self.current_tab_index:
            return
        # Hide the current tab contents and show the selected one
        if self.current_tab_index > -1:
            self.tabs_contents[self.current_tab_index].pack_forget()
        self.tabs_contents[index].pack(fill="both", expand=True)
        # Update current tab index
        self.current_tab_index = index

        current_tab_content = self.tabs_contents[self.current_tab_index]
        mode = ctk.get_appearance_mode().lower()
        if current_tab_content.sql_textbox_theme != mode:
            current_tab_content.change_sql_textbox_theme(mode)
        if current_tab_content.results_section.table_section.table_theme != mode:
            current_tab_content.results_section.table_section.change_table_theme(mode)

    def toggle_tabs(self):
        # Toggle the visibility of the tabs_buttons_frame (collapsing and expanding it)
        if (
            self.tabs_buttons_frame.winfo_ismapped()
        ):  # If tabs_buttons_frame is currently visible
            self.tabs_buttons_frame.pack_forget()  # Hide the tab frame
        else:
            self.content_frame.pack_forget()
            # First, ensure the tabs_buttons_frame is packed from the left side
            self.tabs_buttons_frame.pack(side="left", fill="y", padx=10, pady=10)
            self.content_frame.pack(
                side="left", fill="both", expand=True, padx=10, pady=10
            )
            # Repack the main frame to avoid layout shifts
            self.update_idletasks()

    def move_tab(self, index, direction):
        tabs_length = len(self.tabs_buttons)
        if tabs_length == 1:
            return
        # to handle movements as circular list
        new_index = (index + direction) % len(self.tabs_buttons)
        if self.current_tab_index == index:
            self.current_tab_index = new_index
        elif self.current_tab_index == new_index:
            self.current_tab_index = index
        button_x: TabButton = self.tabs_buttons[index]
        button_y: TabButton = self.tabs_buttons[new_index]

        # region Swap tabs buttons title and content
        title_x, title_y = (
            button_x.title,
            button_y.title,
        )
        button_x.update_title_and_text(title_y)
        button_y.update_title_and_text(title_x)

        self.tabs_contents[index], self.tabs_contents[new_index] = (
            self.tabs_contents[new_index],
            self.tabs_contents[index],
        )

        # endregion
