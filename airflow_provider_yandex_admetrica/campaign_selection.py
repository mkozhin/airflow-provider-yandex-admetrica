"""The campaigns one statistics export walks, as a value of its own.

An advertiser's campaign list is the whole cabinet, and most of a cabinet is
history: a campaign archived a year ago answers every day of a report with
nothing and costs a request all the same.  A hundred campaigns over a month are
three thousand requests for the numbers of a handful.  This module holds the one
rule that decides which campaigns a day is walked over, and the one place where
the text a run form carries becomes that rule.

Everything here is pure — no HTTP, no Airflow, no connection.  The operator
builds a selection before it builds the hook, so a selection worded wrong costs
no request at all, and the hook is handed the finished value.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

#: Walk only the campaigns the cabinet calls active.  The default, because the
#: numbers a schedule collects day after day are the numbers of campaigns that
#: are running.
SCOPE_ACTIVE = "active"

#: Walk the whole list, whatever each campaign's status says.  What a backfill
#: of a past period asks for: a campaign archived since then still gave real
#: rows on the days being collected.
SCOPE_ALL = "all"

#: The value of a campaign's ``status`` field that :data:`SCOPE_ACTIVE` keeps.
#: The management API documents two words for that field, ``"active"`` and
#: ``"archived"``, so this one keeps the campaigns that are still running.  The
#: comparison is written down once, here, and made forgiving about case and
#: surrounding space, which is what an answer wording it otherwise would need.
ACTIVE_STATUS = "active"

#: The scopes :meth:`CampaignSelection.parse` accepts, in the spelling a
#: refusal offers back.
_SCOPES = (SCOPE_ACTIVE, SCOPE_ALL)

#: How long a rejected number may be, written out, and still be quoted back.
#: Long enough for the numbers a campaign id is written as, and short of the
#: runs of digits an account number, a card or a long numeric secret is written
#: as.  A PIN and a one-time code fit under the bound and are quoted like an id:
#: that shape is exactly the shape ``campaign_ids`` asks for, so what the bound
#: buys is a bounded price rather than none at all.
_QUOTABLE_LENGTH = 8

#: The one shape a refusal repeats: ASCII digits, optionally signed.  The rule
#: about what may leave this module keeps a pattern of its own, so that it holds
#: wherever the vocabulary a parameter accepts goes.
_QUOTABLE_TEXT_RE = re.compile(r"[+-]?[0-9]+")

#: A campaign id written as text: the ASCII digits of the number itself, with no
#: sign and no leading zero.  Python's ``int`` is wider than that — it reads
#: ``1_2`` as twelve and the Arabic-Indic ``١٢`` as twelve as well — and a
#: mistyped id that becomes a different existing campaign is collected in
#: silence where a refused one is not.  The one spelling this admits is the one
#: the number is written back in, which is what makes an accepted id the very
#: text the caller typed; :func:`_as_id` says what that buys.
_ID_TEXT_RE = re.compile(r"0|[1-9][0-9]*")

#: The characters a container written out opens with.  A list and a set are
#: what a DAG writes, and a tuple is what a rendered ``{{ params.campaigns }}``
#: arrives as whenever the value behind it is one, so all three open a
#: container here.
_CONTAINER_OPENERS = ("[", "{", "(")

#: What to write instead, told in the vocabulary of ``campaign_ids``.  An id is
#: digits, and the two spellings that reach this module are the container and
#: the comma-separated line; the leading zero is named because Python reads no
#: number written that way and a hand-typed run form writes one.
_IDS_ADVICE = (
    "Inside the brackets an id is written as its own digits, quoted or not — "
    "[999, 1000] or ['999', '1000'] — with every bracket and quote closed and "
    "no leading zero, because 0999 is no number to Python. Or write the ids as "
    "one line with commas between them: 999, 1000."
)

#: What to write instead, told in the vocabulary of ``campaign_names``.  A name
#: is free text, so the container spelling is the quoted one — the only
#: spelling that says a name holding a comma — and the comma-separated line is
#: the one a run form is typed in.
_NAMES_ADVICE = (
    "Inside the brackets every name is written in quotes — ['[EN] Summer', "
    "'Autumn'] — with every bracket and quote closed. Or write the names as "
    "one line with commas between them: [EN] Summer, Autumn."
)


def _written(value: object) -> str | None:
    """Return *value* written out, or ``None`` where it cannot be written.

    CPython renders an integer only up to ``sys.get_int_max_str_digits`` digits
    and refuses a longer one with a ``ValueError`` worded in terms of that
    setting.  Every sentence this module lets out is its own, so the length that
    cannot be written is answered here as an absence and described in the
    wording of a campaign id, rather than escaping as the interpreter's message
    about a limit nobody set.
    """
    try:
        return repr(value)
    except ValueError:
        return None


def _is_quotable(written: str) -> bool:
    """Whether a value written out this way may be repeated in a refusal.

    A short run of ASCII digits, optionally signed: :data:`_QUOTABLE_TEXT_RE`
    within :data:`_QUOTABLE_LENGTH`.  The bound is the whole of the safety here
    — a run of digits long enough to be an account number, a card or a
    thousand-digit integer is over it — and it is asked of the written form,
    so that a number given as one and a number typed as text are held to the
    same length.
    """
    return len(written) <= _QUOTABLE_LENGTH and bool(
        _QUOTABLE_TEXT_RE.fullmatch(written)
    )


def _describe_rejected(value: object, *, quote_numbers: bool = False) -> str:
    """Name the value a refusal is about, quoting it only where that is safe.

    ``campaign_ids``, ``campaign_names`` and ``campaign_scope`` are filled in
    from a run form and hold whatever was typed there, a credential among the
    possibilities, while this text is read from a task log and from a traceback.
    Parsing runs before the hook is built, so the token is not read yet and the
    masking gate that guards every other channel of this provider has nothing to
    clean against: describing rather than quoting is what keeps the channel safe
    here.  By default nothing is quoted at all — a value travels as its type and
    its length, ``'hunter2'`` and a mistyped ``'12a'`` alike.

    *quote_numbers* opens the one exception, and only the caller that means a
    number asks for it.  A campaign id **is** a number, so a refusal there that
    never showed the digits it could not read would leave the operator unable to
    tell a typo from the id of another cabinet; and such a value is refused for
    how much it is rather than for how it is written — ``'0'`` and ``'-1'`` are
    the right shape for a campaign id and still no campaign id — so a length
    alone would name nothing to go and fix.  What is quoted is bounded by
    :func:`_is_quotable`, because a purely numeric secret exists: a PIN or a
    one-time code is a run of digits too, and the bound is what keeps a long one
    out.  Where a number is not the meaning of the parameter — a scope is a
    word, a name is text — digits carry no diagnosis worth that risk and this
    stays off.
    """
    if type(value) is bool:
        return f"a bool of {value}"
    if type(value) in (int, float):
        written = _written(value)
        if written is None:
            return f"a {type(value).__name__} of more digits than can be written"
        if quote_numbers and _is_quotable(written):
            return f"a {type(value).__name__} of {written}"
        return f"a {type(value).__name__} of {len(written)} character(s)"
    if type(value) is str:
        if quote_numbers and _is_quotable(value):
            return repr(value)
        return f"a str of {len(value)} character(s)"
    return f"a value of type {type(value).__name__}"


def check_one_of(parameter: str, value: object, allowed: Sequence[str]) -> str:
    """Return which word of *allowed* *value* names, or refuse it.

    The one rule by which a parameter of this provider is held to a fixed
    vocabulary.  Surrounding space and case are dropped, because such a value
    arrives rendered from a run form where neither of them is visible, and the
    refusal offers every accepted spelling back, so a misspelt word is answered
    with the word that was meant.

    What was given is described by :func:`_describe_rejected` rather than
    quoted: the parameters read this way are filled in from outside, the refusal
    is read from a task log and from a traceback, and this runs before a hook
    exists to mask anything.
    """
    if type(value) is str:
        word = value.strip().lower()
        if word in allowed:
            return word
    offered = " or ".join(f'"{word}"' for word in allowed)
    raise ValueError(
        f"{parameter} must be {offered}; {_describe_rejected(value)} was given."
    )


def _is_blank(element: object) -> bool:
    """Whether an element of a written-out list says nothing at all."""
    return type(element) is str and not element.strip()


def _split_on_commas(text: str) -> tuple:
    """Return the elements a comma-separated line names, space dropped."""
    return tuple(part.strip() for part in text.split(","))


def _meant_as_a_container(text: str) -> bool:
    """Whether bracketed text asks to be read as a written-out container.

    The rule that tells "the caller wrote a container" from "the caller wrote
    text that happens to carry brackets", written down here and nowhere else.

    A container is a bracket opening straight onto a quote: ``"['Summer',
    'Winter']"``, ``'["Summer"]'``, ``"{'Summer'}"``, ``"('Summer',)"``.  That
    is the shape Jinja renders a list, a set and a tuple of campaign names in,
    and it is a shape no cabinet gives a campaign.  Such text has to parse, and
    one whose tail went missing is refused rather than split on its commas: the
    two junk names ``"['Summer'"`` and ``"'Winter'"`` would make an explicit
    selection, which overrides the scope and then matches nothing — under
    ``on_missing_campaign="warn"`` a green task and an empty day.

    Everything else bracketed is free text.  An ad cabinet tags a campaign with
    its locale, its market and its year in brackets, so a name opens and closes
    on one as a matter of course — ``"[MSK] Лето [2026]"``, ``"[Test]"``,
    ``"[Промо] Осень]"`` — and a comma-separated line of such names, the line an
    operator types into a run form, is read as the names it spells out.

    A campaign id is digits and has no bracketed spelling of its own, so
    ``campaign_ids`` never asks this: a bracket there opens a container however
    it is written, and text that does not parse as one is refused by name.
    """
    return text[1:].lstrip().startswith(("'", '"'))


def _is_container(value: object) -> bool:
    """Whether *value* already holds campaigns one by one.

    Anything that keeps its elements and knows how many it has counts: the list
    a DAG usually writes, a tuple, a set, the ``range`` an unbroken block of ids
    is written as, a ``deque`` a caller passes along.  The parameters take a
    ``Collection`` and this is what reads one.

    Text is left out although it is a collection of characters: a line has its
    own two spellings, read further down.  A mapping is left out too — walking
    it would take its keys for campaigns — so it arrives at the check that
    refuses it by what it is.
    """
    return isinstance(value, Collection) and not isinstance(
        value, (str, bytes, bytearray, Mapping)
    )


def _written_elements(text: str, tree: ast.expr) -> tuple:
    """Return the elements of a written-out container, whole numbers as spelled.

    A whole number read out of a literal keeps only its value: ``1_2`` and
    ``0x0c`` are twelve by the time :func:`ast.literal_eval` is done with them,
    and ``+999`` is nine hundred and ninety-nine.  A parameter whose elements
    are whole numbers is held to one spelling of each of them, and the spelling
    is in the source rather than in the value, so every element that reads as
    one comes back as the text it was written as and is judged as text.

    Everything else comes back as the value it names, and a number written with
    a fractional part among it: such a number is no campaign id in any
    spelling, and coming back as itself is what lets a refusal name it a float
    rather than describe the characters it was typed as.

    The elements come in the order the container was written in, so a set is
    read the way a list is.
    """
    if not isinstance(tree, (ast.List, ast.Tuple, ast.Set)):
        return (ast.literal_eval(tree),)
    elements = []
    for node in tree.elts:
        value = ast.literal_eval(node)
        written = ast.get_source_segment(text, node)
        if type(value) is int and written is not None:
            elements.append(written)
        else:
            elements.append(value)
    return tuple(elements)


def _as_elements(
    parameter: str,
    value: object,
    *,
    may_be_text: bool,
    numbers_as_written: bool = False,
) -> tuple:
    """Return the elements *value* names, whichever spelling it arrived in.

    The same parameter reaches this module as a collection of its own — from a
    DAG that wrote one — and as text, from a rendered ``{{ params.… }}``, and
    both spellings mean the same selection.  Text is read two ways: a container
    written out, or a line split on commas.

    A written-out container is read by :func:`ast.literal_eval` rather than
    ``json.loads`` because Jinja renders a collection as its Python repr: an
    array parameter holding ``["Summer"]`` arrives as ``['Summer']``, single
    quotes and all, which is a syntax error to JSON, and a tuple arrives as
    ``('Summer',)``.  ``literal_eval`` reads those spellings, reads honest JSON
    with double quotes, reads ``[123, 534, 234]``, reads a written-out set the
    way a native one is taken, and evaluates literals rather than code.

    A mapping is refused for what it is: rendered into a parameter that asks for
    a list it names no campaign, while splitting it on commas would turn its
    punctuation into campaign names that quietly match nothing.

    *may_be_text* says whose vocabulary the text is read in.  Under it a bracket
    opens a container only where :func:`_meant_as_a_container` says so, and
    every other bracketed line is a name carrying brackets of its own; without
    it a bracket opens a container outright, because an id has no bracketed
    spelling.  That function holds the whole of the rule, and the refusal is
    worded in the vocabulary of the parameter it refuses.

    *numbers_as_written* says what a number inside a written-out container is
    worth.  A parameter whose elements are numbers asks for it, because
    evaluating ``[1_2]`` leaves twelve and nothing of the two characters that
    were never a twelve: under it every element that reads as a number comes
    back as :func:`_written_elements` found it spelled, and the caller judges
    the spelling.

    Text that is empty once trimmed names nothing, which is how an empty
    ``{{ params.campaign_ids }}`` says "no explicit selection" instead of "a
    selection matching nothing".

    Anything else is one element on its own, so that a single campaign may be
    written as itself.  What such an element is worth is the caller's to judge:
    this function reads the shape of a list, never the meaning of a campaign.
    """
    if value is None:
        return ()
    if _is_container(value):
        return tuple(value)
    if type(value) is not str:
        return (value,)
    text = value.strip()
    if not text:
        return ()
    if text.startswith(_CONTAINER_OPENERS) and (
        not may_be_text or _meant_as_a_container(text)
    ):
        try:
            tree = ast.parse(text, mode="eval").body
            parsed = ast.literal_eval(tree)
        except (ValueError, SyntaxError, TypeError):
            # ``literal_eval`` refuses a malformed container with any of the
            # three, and none of them is a subclass of another: text that is not
            # a literal at all is a ``SyntaxError``, a literal holding something
            # that is not one is a ``ValueError``, and a set written over an
            # unhashable element — ``{[1]}`` — is a ``TypeError`` raised while
            # the set is being built.  Every refusal of this module is a
            # ``ValueError``, so all three become one here.  What is left
            # uncaught is left so on purpose: ``MemoryError`` and
            # ``RecursionError`` say the interpreter is out of room, which is
            # not a statement about the selection and must not be worded as one.
            # ``from None`` breaks the chain because
            # ``SyntaxError`` keeps the offending text in its ``text`` attribute
            # and a traceback prints it in full under "During handling of the
            # above exception": the value this refusal is careful to describe
            # rather than quote would ride out in the frame next to it.
            raise ValueError(
                f"{parameter} holds {_describe_rejected(value)}, which is "
                f"written as a container and does not read as a list of "
                f"campaigns. {_NAMES_ADVICE if may_be_text else _IDS_ADVICE}"
            ) from None
        if not _is_container(parsed):
            raise ValueError(
                f"{parameter} holds {_describe_rejected(value)}, which reads as "
                f"a {type(parsed).__name__} rather than a list of campaigns. "
                f"{_NAMES_ADVICE if may_be_text else _IDS_ADVICE}"
            )
        if numbers_as_written:
            return _written_elements(text, tree)
        return tuple(parsed)
    return _split_on_commas(text)


def _as_id(value: object) -> int:
    """Return the campaign id *value* names, or refuse it.

    An id written as text is an ordinary way to write one: a run form is typed
    by hand, and a comma-separated list is nothing but text to begin with.  Such
    text is held to :data:`_ID_TEXT_RE`, the digits a person means by a number,
    rather than to everything ``int`` accepts.  A flag is not a number here even
    though Python counts one as an ``int``, and neither is a value at or below
    zero: the ids the API issues start above it.  Nor is a number longer than
    :func:`_written` can write out: an id that no message of this provider could
    name is an id no request could be built from either.

    A spelling that says the same number some other way — ``"+999"``,
    ``"000999"``, ``"1_2"``, ``"0x0c"`` — is refused rather than read as the
    number it evaluates to.  What comes back here is an ``int``, which is what a
    campaign is matched by, and every later mention of that id is the ``int``
    written out again: admitting only the spelling the number is written back in
    keeps those two the same text, so that an id repeated somewhere else is
    repeated in the shape it was typed in and can be recognised there for what
    it holds.  A run form filled in with a credential is the case that turns on
    it, and ``1_2`` selecting campaign twelve in silence is the case that turns
    on it for a caller who typed an id and meant it.

    The rule reaches every id that arrives as text, in all three spellings text
    comes in: an id on its own, an id in a comma-separated line, and an id
    inside a written-out container, where :func:`_written_elements` keeps each
    number as it was spelled so that the rule is asked of the source rather than
    of a value an evaluation already flattened.  What keeps a rendered value text
    at all is the operator, which renders its own template fields as the
    characters they rendered to: a DAG that renders templates as native objects
    would otherwise hand this module the ``int`` twelve where the run form was
    filled in with ``1_2``.

    An ``int`` is the number a Python literal names, written where the DAG is
    read: ``campaign_ids=[999]`` is the DAG author's own writing and has no
    second spelling to have arrived in.  A :class:`CampaignSelection` built by
    its constructor instead of by :meth:`CampaignSelection.parse` says the same
    thing about the ids it is given.

    This is the one refusal of the module that repeats a number back, because a
    number is what the parameter means and a bounded one is what
    :func:`_describe_rejected` lets out.
    """
    parsed = None
    if type(value) is int:
        parsed = value
    elif type(value) is str and _ID_TEXT_RE.fullmatch(value.strip()):
        try:
            parsed = int(value.strip())
        except ValueError:
            # CPython reads only so long a run of digits as an integer, and the
            # message it words the refusal in talks about
            # ``sys.set_int_max_str_digits``.  A run that long is no campaign
            # id, and the sentence below says so in the terms of this module.
            parsed = None
    if parsed is None or parsed <= 0 or _written(parsed) is None:
        raise ValueError(
            f"campaign_ids holds "
            f"{_describe_rejected(value, quote_numbers=True)}, which is not a "
            f"campaign id. A campaign is asked for by a positive whole number "
            f"written as its own digits: 999, and neither +999 nor 000999, "
            f"because a sign and a leading zero say the same number in another "
            f"spelling and an id is repeated later in the spelling it arrived "
            f"in. The export stops here rather than walking a selection that "
            f"names nothing."
        )
    return parsed


def _as_name(value: object) -> str:
    """Return the campaign name *value* names, or refuse it.

    A name is text and only text.  A number here would match no campaign of the
    list and, being an explicit selection, would override the scope on its way
    to collecting nothing — so it is refused where it is written rather than
    where its absence would show.
    """
    if type(value) is not str:
        raise ValueError(
            f"campaign_names holds {_describe_rejected(value)}, which is not a "
            f"campaign name. A campaign is named by text, so the export stops "
            f"here rather than walking a selection that names nothing."
        )
    return value.strip()


def _as_scope(value: object) -> str:
    """Return the scope *value* names, or refuse it.

    ``None`` is a scope nobody wrote down, which is the default rather than a
    mistake: a DAG that says nothing about the campaigns walks the running ones.
    Every other value is held to the two words by :func:`check_one_of`.
    """
    if value is None:
        return SCOPE_ACTIVE
    return check_one_of("campaign_scope", value, _SCOPES)


def _names_campaign(name: str, campaign: dict) -> bool:
    """Whether *name*, as a selection words it, names this campaign.

    The one place a name is compared, asked by :meth:`CampaignSelection.matching`
    and by :meth:`CampaignSelection.missing` alike.  Two comparisons written
    twice would sooner or later disagree, and the disagreement has a shape: a
    campaign whose name carries a trailing space would be collected by one and
    reported unfound by the other — a WARNING about a campaign that is being
    exported as it is written, and, under ``on_missing_campaign="fail"``, a task
    failed over a selection that is entirely correct.

    Surrounding space is dropped on both sides, because it is invisible in a run
    form and in a cabinet alike.  Case is not: two campaigns of one advertiser
    may differ by it, and a cabinet is free to name them so.

    A campaign's name arrives worded exactly as the answer put it, which means
    it may be absent or be something other than text.  Such a campaign is named
    by nothing: the field is read for text and compared only as text, so an
    unnamed campaign falls out of the selection instead of stopping the walk
    with an ``AttributeError``.
    """
    listed = campaign.get("name")
    return type(listed) is str and listed.strip() == name.strip()


def _has_selected_id(campaign: dict, ids: frozenset[int]) -> bool:
    """Whether the campaign's own id is one of the ids asked for."""
    campaign_id = campaign.get("campaign_id")
    return type(campaign_id) is int and campaign_id in ids


def _is_active(campaign: dict) -> bool:
    """Whether the cabinet calls this campaign active.

    The management API words this field ``"active"`` or ``"archived"``, and the
    comparison drops surrounding space and case on top of that: ``"Active"`` and
    ``"active "`` say the same thing about a campaign, and a scope that read one
    and not the other would silently collect an empty day.  A status that is not
    text says nothing about the campaign, and a campaign nothing is known about
    is not walked under a scope that asks for the running ones.
    """
    status = campaign.get("status")
    return type(status) is str and status.strip().lower() == ACTIVE_STATUS


@dataclass(frozen=True)
class CampaignSelection:
    """The campaigns a statistics export walks, and nothing else.

    Three fields and no behaviour beyond reading them: a scope naming a rule,
    and — overriding it — the ids and names of campaigns asked for one by one.
    Naming a campaign means wanting that campaign, an archived one included, so
    an explicit selection is not narrowed further by the scope; that is what
    makes a single archived campaign re-collectable without also remembering to
    change a second parameter.

    ``scope`` is a string rather than a flag on purpose.  A rendered
    ``{{ params.collect_all }}`` is the string ``"False"``, and every non-empty
    string is true: a boolean parameter would quietly switch itself on, and the
    only trace would be the number of requests spent, not an error.  A string
    has no such trap — a mistyped ``"activ"`` is refused by name.

    The instance built by the no-argument constructor is the default of the
    whole provider: active campaigns only, nothing named explicitly.
    """

    scope: str = SCOPE_ACTIVE
    ids: frozenset[int] = frozenset()
    names: frozenset[str] = frozenset()

    @classmethod
    def parse(
        cls,
        *,
        scope: object = SCOPE_ACTIVE,
        ids: object = None,
        names: object = None,
    ) -> CampaignSelection:
        """Read a selection out of the values a task run carries.

        Every argument arrives either as the Python value a DAG wrote or as the
        text a template rendered, and both are read into the same value here —
        the single point where the outside spelling of a selection ends.

        Whitespace-only elements are dropped rather than refused.  A trailing
        comma and a blank line in a hand-typed list are not mistakes worth
        failing a run over, while keeping them would be worse than either: an
        empty name matches no campaign, yet it makes the selection explicit,
        which overrides the scope and sends the day off collecting nothing.

        Every refusal is a :class:`ValueError`, raised before a hook exists and
        therefore before a single request is spent.
        """
        parsed_ids = frozenset(
            _as_id(element)
            for element in _as_elements(
                "campaign_ids", ids, may_be_text=False, numbers_as_written=True
            )
            if not _is_blank(element)
        )
        parsed_names = frozenset(
            _as_name(element)
            for element in _as_elements("campaign_names", names, may_be_text=True)
            if not _is_blank(element)
        )
        return cls(scope=_as_scope(scope), ids=parsed_ids, names=parsed_names)

    @property
    def is_explicit(self) -> bool:
        """Whether campaigns were asked for by name or by id.

        True means the scope is not consulted at all: what was named is what is
        walked.
        """
        return bool(self.ids or self.names)

    def _keeps(self, campaign: dict) -> bool:
        """Whether this selection walks *campaign*.

        The whole rule, asked of one campaign at a time.  An explicit selection
        wins outright: naming a campaign means wanting that campaign, an
        archived one included, and having to remember a second parameter to get
        it would make the first one a trap.  So where ids or names are given the
        scope is not consulted at all, and a campaign is kept when its id is
        among the ids **or** its name is among the names — the two are one
        selection worded two ways, not two filters narrowing each other.

        Under :data:`SCOPE_ALL` the status field is not read at all; under
        :data:`SCOPE_ACTIVE` it is compared to :data:`ACTIVE_STATUS`.
        """
        if self.is_explicit:
            return _has_selected_id(campaign, self.ids) or any(
                _names_campaign(name, campaign) for name in self.names
            )
        return self.scope == SCOPE_ALL or _is_active(campaign)

    def matching(self, campaigns: Sequence[dict]) -> list[dict]:
        """Return the campaigns of *campaigns* this selection walks.

        The list is walked once and each campaign judged once, so the order the
        cabinet listed them in survives and a campaign named twice over — by id
        and by name both — is walked a single time.  A duplicate would double
        every row of its day.
        """
        return [campaign for campaign in campaigns if self._keeps(campaign)]

    def partition(self, campaigns: Sequence[dict]) -> tuple[list[dict], list[dict]]:
        """Return the campaigns of *campaigns* this selection walks and skips.

        The same rule as :meth:`matching` and its inverse, answered together, so
        that a caller reporting on how much of a cabinet a day covers reads both
        halves from the one module the rule lives in.  Each list keeps the order
        the cabinet listed the campaigns in, and every campaign lands in exactly
        one of them.
        """
        selected: list[dict] = []
        skipped: list[dict] = []
        for campaign in campaigns:
            (selected if self._keeps(campaign) else skipped).append(campaign)
        return selected, skipped

    def missing(
        self, campaigns: Sequence[dict]
    ) -> tuple[frozenset[int], frozenset[str]]:
        """Return what this selection named that *campaigns* does not hold.

        Ids and names come back apart because the message built from them tells
        them apart: an id nobody recognises and a name nobody recognises are
        different mistakes, and a caller who wrote one of each deserves to read
        both.

        A campaign deleted in the interface leaves the cabinet's list for good,
        so a name here is as often a campaign that no longer exists as it is a
        typo; which of the two it is belongs to the caller's policy, not to this
        function, which only says what was not found.

        Names are compared by :func:`_names_campaign`, the same function
        :meth:`matching` asks, so nothing is ever both collected and reported
        unfound.
        """
        listed_ids = frozenset(
            campaign["campaign_id"]
            for campaign in campaigns
            if type(campaign.get("campaign_id")) is int
        )
        missing_ids = self.ids - listed_ids
        missing_names = frozenset(
            name
            for name in self.names
            if not any(_names_campaign(name, campaign) for campaign in campaigns)
        )
        return missing_ids, missing_names
