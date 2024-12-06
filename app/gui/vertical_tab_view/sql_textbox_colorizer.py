import re
import customtkinter as ctk


class Colorizer:
    COLOR_SCHEMES = {
        "dark": {
            "background": "#1E1E1E",
            "text": "#D4D4D4",
            "keyword": "#569CD6",  # Light blue
            "string": "#CE9178",  # Light orange
            "comment": "#6A9955",  # Light green
            "number": "#B5CEA8",  # Light greenish
            "brackets": {
                "square": "#C678DD",  # Purple
                "curly": "#F9A825",  # Yellow
                "round": "#61AFEF",  # Blue
                "angle": "#E5C07B",  # Light yellow
            },
        },
        "light": {
            "background": "#FFFFFF",
            "text": "#000000",
            "keyword": "#0000FF",  # Dark blue
            "string": "#A31515",  # Dark red
            "comment": "#008000",  # Green
            "number": "#098658",  # Dark teal
            "brackets": {
                "square": "#800080",  # Purple
                "curly": "#FF8C00",  # Dark orange
                "round": "#0000CD",  # Medium blue
                "angle": "#DAA520",  # Goldenrod
            },
        },
    }
    KEYWORDS = [
        "SELECT",
        "FROM",
        "INTO",
        "WHERE",
        "LIKE",
        "INSERT",
        "AND",
        "ORDER",
        "OR",
        "NOT",
        "DISTINCT",
        "BY",
        "ASC",
        "DESC",
        "LIMIT",
        "TAIL",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
    ]
    TAGS = [
        "keyword",
        "string",
        "comment",
        "number",
        "square_brackets",
        "curly_brackets",
        "round_brackets",
        "angle_brackets",
    ]

    def highlight_syntax(textbox_widget: ctk.CTkTextbox, mode: str):
        colors = Colorizer.COLOR_SCHEMES[mode]
        # Update text widget colors
        textbox_widget.configure(
            fg_color=colors["background"], text_color=colors["text"]
        )

        # Remove existing tags

        for tag in Colorizer.TAGS:
            textbox_widget.tag_remove(tag, "1.0", "end")

        # Get text content
        text_content = textbox_widget.get("1.0", "end")

        # Create keywords regex pattern
        keywords_pattern = (
            r"\b(" + "|".join(re.escape(kw) for kw in Colorizer.KEYWORDS) + r")\b"
        )

        # Syntax highlighting patterns
        patterns = [
            # Keywords (case-insensitive, avoiding bracketed column names)
            (
                "keyword",
                keywords_pattern,
                lambda match: not (
                    text_content[max(0, match.start() - 1) : match.start()].startswith(
                        "["
                    )
                    and text_content[match.end() : match.end() + 1].startswith("]")
                ),
            ),
            # Strings (both single and double quotes)
            ("string", r'".*?"|\'.*?\'', None),
            # Comments (multi-line)
            ("comment", r"/\*.*?\*/", None),
            # Numbers
            ("number", r"\b\d+\b", None),
            # Bracket types
            ("square_brackets", r"\[.*?\]", None),
            ("curly_brackets", r"\{.*?\}", None),
            ("round_brackets", r"\(.*?\)", None),
            ("angle_brackets", r"<.*?>", None),
        ]

        # Apply highlighting
        for tag_name, pattern, condition in patterns:
            for match in re.finditer(pattern, text_content, re.DOTALL | re.IGNORECASE):
                # Apply condition if provided
                if condition and not condition(match):
                    continue

                start, end = match.start(), match.end()
                start_index = f"1.0+{start}c"
                end_index = f"1.0+{end}c"

                # Add tag for highlighting
                textbox_widget.tag_add(tag_name, start_index, end_index)

        # Configure tag colors
        textbox_widget.tag_config("keyword", foreground=colors["keyword"])
        textbox_widget.tag_config("string", foreground=colors["string"])
        textbox_widget.tag_config("comment", foreground=colors["comment"])
        textbox_widget.tag_config("number", foreground=colors["number"])

        # Configure bracket colors
        textbox_widget.tag_config(
            "square_brackets", foreground=colors["brackets"]["square"]
        )
        textbox_widget.tag_config(
            "curly_brackets", foreground=colors["brackets"]["curly"]
        )
        textbox_widget.tag_config(
            "round_brackets", foreground=colors["brackets"]["round"]
        )
        textbox_widget.tag_config(
            "angle_brackets", foreground=colors["brackets"]["angle"]
        )
