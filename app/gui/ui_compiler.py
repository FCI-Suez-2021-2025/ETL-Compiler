import customtkinter as ctk

from app.gui.vertical_tab_view.vertical_tab_view import VerticalTabView


# Initialize customtkinter
ctk.set_appearance_mode("Light")  # Modes: "System" (default), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Themes: "blue" (default), "green", "dark-blue"


class UICompiler(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("QueryFlow")
        self.geometry("800x500")
        self.set_window_properties()
        self.add_children_widgets()

    def update_widgets_manual_theme(self):
        if self.vertical_tab_view.current_tab_index == -1:
            return
        mode = "dark" if self.theme_switch.get() == 1 else "light"

        current_tab_content = self.vertical_tab_view.tabs_contents[
            self.vertical_tab_view.current_tab_index
        ]
        current_tab_content.change_sql_textbox_theme(mode)
        current_tab_content.results_section.table_section.change_table_theme(mode)

    def add_children_widgets(self) -> None:
        # Create a top frame for controls (buttons and dropdown)
        self.top_frame = ctk.CTkFrame(self)
        self.toggle_button = ctk.CTkButton(
            self.top_frame, text="Collapse Tabs", command=self.toggle_tabs
        )
        # Add Tab button
        add_tab_button = ctk.CTkButton(
            self.top_frame, text="Add Tab", command=self.add_tab
        )
        # Theme toggle switch
        self.theme_switch = ctk.CTkSwitch(
            self.top_frame, text="Dark Mode", command=self.toggle_theme
        )
        self.top_frame.pack(
            side="top", fill="x", padx=20, pady=5
        )  # Full width of the window
        self.toggle_button.pack(side="left", padx=10, pady=5)
        add_tab_button.pack(side="left", padx=5, pady=5)
        self.theme_switch.pack(side="left", padx=10, pady=5)

        # Create a vertical tab view for queries
        self.vertical_tab_view = VerticalTabView(self)
        self.vertical_tab_view.pack(fill="both", expand=True, padx=20, pady=10)

    def set_window_properties(self) -> None:
        """
        Set window properties including app icon for different operating systems.

        Handles icon setting for Windows and Linux with improved error handling.
        """
        import platform

        try:
            icon_path = "./app/gui/Assets/icon.ico"

            # Windows-specific icon setting
            if platform.system() == "Windows":
                try:
                    from ctypes import windll
                    windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                        "QueryFlow Desktop App.1.0"
                    )
                    self.iconbitmap(icon_path)
                except Exception as e:
                    print(f"Windows icon setting failed: {e}")

            # Linux-specific icon setting
            elif platform.system() == "Linux":
                try:
                    # Use PIL for better image handling
                    from PIL import Image, ImageTk

                    # Open image with PIL to handle potential file format issues
                    icon_image = Image.open(icon_path)

                    # Convert to PhotoImage
                    photo_icon = ImageTk.PhotoImage(icon_image)

                    # Set window icon
                    self.wm_iconphoto(True, photo_icon)
                except ImportError:
                    print("PIL (Pillow) library is required for Linux icon setting. Install with 'pip install pillow'")
                except FileNotFoundError:
                    print(f"Icon file not found: {icon_path}")
                except Exception as e:
                    print(f"Linux icon setting failed: {e}")

            # macOS support
            elif platform.system() == "Darwin":
                print("macOS icon setting not implemented")

        except Exception as e:
            print(f"Unexpected error in set_window_properties: {e}")

    def add_tab(self):
        self.vertical_tab_view.add_tab()

    def toggle_tabs(self):
        self.vertical_tab_view.toggle_tabs()

    def toggle_theme(self):
        current_mode = ctk.get_appearance_mode()
        new_mode = "Dark" if current_mode == "Light" else "Light"
        ctk.set_appearance_mode(new_mode)
        self.update_widgets_manual_theme()


if __name__ == "__main__":
    app = UICompiler()
    app.mainloop()
