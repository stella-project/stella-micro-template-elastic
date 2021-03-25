import xx_sent_ud_sm
import en_core_sci_lg

nlp_uni = xx_sent_ud_sm.load()
nlp_sci = en_core_sci_lg.load()

# UNIVERSAL
def is_token_allowed_uni(token):
    '''
         Only allow valid tokens which are not stop words
         and punctuation symbols.
    '''
    if not token or not token.text.strip() or token.is_stop or token.is_punct:
        return False
    return True


def preprocesstoken_uni(token):
    # Reduce token to its lowercase lemma form
    return token.lemma_.strip().lower()


def tokenize_uni(x):
    try:
        return str([preprocesstoken_uni(token) for token in nlp_uni(x) if is_token_allowed_uni(token)])
    except:
        return str([])


def tokenize_string_uni(x):
    return ",".join([preprocesstoken_uni(token) for token in nlp_sci(x) if is_token_allowed_uni(token)])



# SCIENTIFIC
def is_token_allowed_sci(token):
    '''
         Only allow valid tokens which are not stop words
         and punctuation symbols.
    '''
    if not token or not token.text.strip() or token.is_stop or token.is_punct:
        return False
    return True


def preprocesstoken_sci(token):
    # Reduce token to its lowercase lemma form
    return token.lemma_.strip().lower()


def tokenize_sci(x):
    try:
        return str([preprocesstoken_sci(token) for token in nlp_sci(x) if is_token_allowed_sci(token)])
    except:
        return str([])


def tokenize_string_sci(x):
    return ",".join([preprocesstoken_sci(token) for token in nlp_sci(x) if is_token_allowed_sci(token)])


def prettify(x):
    return x.replace("[", "").replace("]", "").lstrip("'").rstrip("'").lstrip('"').rstrip('"')


def prettify_v2(x):
    return x.replace("[", "").replace("]", "").replace("'", "").replace('"', "")