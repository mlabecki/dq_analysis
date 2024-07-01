def squeeze(char: str, s: str):
    """
    Removes repeated instances of character char from string s
    """
    while char*2 in s:
        s = s.replace(char*2, char)
    return s


def add_spaces_html(n: int):
    """
    Add n non-breaking spaces inline in an html markup
    """
    spaces = ''
    i = 0
    while i < n:
        spaces += '&nbsp;'
        i+=1

    return spaces

