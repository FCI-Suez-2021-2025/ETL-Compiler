import sqlparse
from typing import Any, Tuple
import customtkinter as ctk


from app.core.result_monad import Success
from app.etl.controllers import compile_to_python, execute_python_code
from app.gui.error_frame.error_frame import ErrorFrame

from app.gui.results_frame.results_frame import ResultsFrame
from app.gui.vertical_tab_view.sql_textbox_colorizer import Colorizer


class TabContent(ctk.CTkFrame):
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
        self.add_children_widget()

        self.sql_textbox_theme = ctk.get_appearance_mode().lower()
        if self.sql_textbox_theme == "dark":
            self.change_sql_textbox_theme("dark")
        else:
            self.sql_textbox_theme = "light"

    def add_children_widget(self):
        self.sql_textbox = ctk.CTkTextbox(
            self, fg_color=("#ffffff", "#1e1e1e"), font=("Consolas", 24)
        )
        self.sql_textbox.bind(
            "<KeyRelease>",
            lambda _: Colorizer.highlight_syntax(
                self.sql_textbox, self.sql_textbox_theme
            ),
        )
        # Button Frame (for Execute, Run, Delete, Up, Down buttons)
        self.btn_frame = ctk.CTkFrame(self, height=40, fg_color="transparent")
        # Execute Button
        self.execute_btn = ctk.CTkButton(
            self.btn_frame,
            text="Execute",
            command=self.execute_python,
            width=80,
            fg_color="#51ab46",
            hover_color="#387731",
        )
        # Run Button
        self.run_btn = ctk.CTkButton(
            self.btn_frame,
            text="Compile",
            command=self.compile_sql,
            width=80,
        )
        self.results_section = ResultsFrame(self)
        self.error_section = ErrorFrame(
            self,
            border_width=4,
            fg_color=("#f9f9fa", "#1d1e1e"),
            border_color=("#cfcfcf", "#333333"),
            height=50,
        )
        self.sql_textbox.pack(fill="both", expand=True, padx=10, pady=5)
        self.btn_frame.pack(fill="x", pady=5, padx=10)
        self.execute_btn.pack(side="left", padx=5)
        self.run_btn.pack(side="left", padx=5)
        self.results_section.pack(fill="both", expand=True, pady=5, padx=10)
        self.results_section.pack_propagate(False)
        self.error_section.pack(fill="x", pady=5, padx=10)
        self.error_section.pack_propagate(False)

    def execute_python(self):
        # Fetch SQL code from the text box
        python_code = self.results_section.python_section.code_textbox.get(
            "1.0", "end-1c"
        ).strip()
        if not python_code:
            self.results_section.python_section.clear_code()
            self.results_section.table_section.clear_table()
            self.error_section.clear_error()
            return
        execution_result = execute_python_code(python_code)
        if isinstance(execution_result, Success):
            # Execution succeeded; display Python code and DataFrame results
            data_frame = execution_result.unwrap()
            self.results_section.table_section.set_table(data_frame)
            self.error_section.clear_error()
        else:
            # Execution failed; display Python code and error in DataFrame section
            execution_error = execution_result.unwrap_error()
            error_message = f"Python Execution Error:\n{execution_error.message}\nTraceback:\n{execution_error.code}"
            self.results_section.table_section.clear_table()
            self.error_section.set_error(error_message)

    def compile_sql(self):
        # Fetch SQL code from the text box
        sql_query = self.sql_textbox.get("1.0", "end-1c").strip()
        sql_query = sqlparse.format(sql_query, reindent=True, strip_whitespace=True)
        # Delete all content
        self.sql_textbox.delete("1.0", "end")
        # Insert text at the beginning (index "1.0")
        self.sql_textbox.insert("1.0", sql_query)
        Colorizer.highlight_syntax(self.sql_textbox, self.sql_textbox_theme)
        if not sql_query:
            self.results_section.python_section.clear_code()
            self.results_section.table_section.clear_table()
            self.error_section.clear_error()
            return

        # Step 1: Compile SQL code to Python
        compilation_result = compile_to_python(sql_query)

        if isinstance(compilation_result, Success):
            python_code = compilation_result.unwrap()
            self.results_section.python_section.set_code(python_code)
            self.error_section.clear_error()
        else:
            # Compilation failed; display error in the Python Code section
            compilation_error = compilation_result.unwrap_error()
            error_message = f"SQL Compilation Error:\n{compilation_error}"
            self.results_section.python_section.clear_code()
            self.error_section.set_error(error_message)
        self.results_section.table_section.clear_table()

    def copy_to_clipboard(self, text: str):
        # Copy the provided text to the clipboard
        self.clipboard_clear()
        self.clipboard_append(text)
        self.update()

    def change_sql_textbox_theme(self, theme: str) -> None:
        self.sql_textbox_theme = theme
        Colorizer.highlight_syntax(self.sql_textbox, theme)
