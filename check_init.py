def solution(s):
    c = s[0]
    if c[0:1].isalpha() and  c[0:1].isupper():
        return "upper"
    elif c[0:1].isalpha() and  c[0:1].islower():
        return "lower"
    elif c[0:1].isnumeric():
        return "digit"
    else:
        return "other"
