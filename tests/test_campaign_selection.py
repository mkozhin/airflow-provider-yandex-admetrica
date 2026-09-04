"""Tests for the campaign selection: what a run form says, and what it means."""

from __future__ import annotations

import dataclasses
import re
import traceback
from collections import deque
from types import MappingProxyType

import pytest

from airflow_provider_yandex_admetrica.campaign_selection import (
    ACTIVE_STATUS,
    SCOPE_ACTIVE,
    SCOPE_ALL,
    CampaignSelection,
)

TOKEN = "y0__xDf" + "MIDDLE-OF-THE-SECRET" + "q9Az"


class TestDefaults:
    """What the no-argument selection is, since it is the provider's default."""

    def test_it_is_active_and_names_nothing(self):
        selection = CampaignSelection()
        assert selection.scope == SCOPE_ACTIVE
        assert selection.ids == frozenset()
        assert selection.names == frozenset()
        assert selection.is_explicit is False

    def test_active_status_is_the_scope_it_keeps(self):
        assert ACTIVE_STATUS == SCOPE_ACTIVE

    def test_it_is_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            CampaignSelection().scope = SCOPE_ALL

    def test_parse_with_nothing_given_is_the_default(self):
        assert CampaignSelection.parse() == CampaignSelection()


class TestParseIds:
    """Every spelling an id list reaches the operator in."""

    @pytest.mark.parametrize(
        "given",
        [
            [123, 534, 234],
            (123, 534, 234),
            {123, 534, 234},
            frozenset({123, 534, 234}),
            "123,534,234",
            "  123 , 534 ,234  ",
            "[123, 534, 234]",
            "[123,534,234]",
            ["123", "534", "234"],
            "123\n,534,\n234",
        ],
    )
    def test_it_reads_the_same_three_campaigns(self, given):
        assert CampaignSelection.parse(ids=given).ids == frozenset({123, 534, 234})

    def test_a_single_id_needs_no_list(self):
        assert CampaignSelection.parse(ids=123).ids == frozenset({123})

    @pytest.mark.parametrize("given", [None, "", "   ", [], ()])
    def test_nothing_given_leaves_the_selection_open(self, given):
        selection = CampaignSelection.parse(ids=given)
        assert selection.ids == frozenset()
        assert selection.is_explicit is False

    @pytest.mark.parametrize("given", [[123, "", 234], "[123, '', 234]", "123,,234", "123, ,234"])
    def test_an_empty_element_is_dropped_rather_than_refused(self, given):
        assert CampaignSelection.parse(ids=given).ids == frozenset({123, 234})

    @pytest.mark.parametrize("given", [range(123, 126), deque([123, 124, 125])])
    def test_any_collection_of_ids_is_read(self, given):
        # The parameter is annotated as a collection and reads as one: a block
        # of ids written as a ``range`` and a ``deque`` passed along both name
        # the campaigns they hold.
        assert CampaignSelection.parse(ids=given).ids == frozenset({123, 124, 125})

    def test_a_mapping_of_ids_is_no_list_of_campaigns(self):
        # Walking it would take its keys for campaigns, so it is refused for
        # what it is.
        with pytest.raises(ValueError, match="campaign_ids"):
            CampaignSelection.parse(ids={123: "\u041b\u0435\u0442\u043e"})

    def test_a_repeated_id_is_held_once(self):
        assert CampaignSelection.parse(ids="123,123").ids == frozenset({123})

    def test_a_written_out_set_is_read_like_a_written_out_list(self):
        assert CampaignSelection.parse(ids="{123, 534}").ids == frozenset({123, 534})

    @pytest.mark.parametrize("given", ["(123, 534)", "(123, 534,)"])
    def test_a_written_out_tuple_is_read_like_a_written_out_list(self, given):
        assert CampaignSelection.parse(ids=given).ids == frozenset({123, 534})

    @pytest.mark.parametrize("given", ["[123]", "{123}", "(123,)"])
    def test_a_written_out_container_of_one_id_names_it(self, given):
        assert CampaignSelection.parse(ids=given).ids == frozenset({123})

    @pytest.mark.parametrize("given", ["[Лето]", "{Лето}", "[123", "(123)"])
    def test_a_bracket_opens_a_container_however_it_is_written(self, given):
        # An id is digits and has no bracketed spelling of its own, so a bracket
        # in ``campaign_ids`` opens a container in every case: text that does
        # not read as one is refused, never split into elements of punctuation.
        with pytest.raises(ValueError, match="campaign_ids"):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize("given", ["1_2", "١٢", " 1 2 ", "12.0", "0x0c"])
    def test_an_id_is_ascii_digits_and_nothing_else(self, given):
        # ``int`` reads the first two of these as twelve, so a mistyped id would
        # become a different campaign that exists instead of a refusal.
        with pytest.raises(ValueError, match="campaign_ids"):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize("given", ["+999", "000999", "007", "+7"])
    def test_an_id_is_written_the_way_the_number_is_written_back(self, given):
        # ``int`` reads every one of these as a campaign that exists, and the
        # id would then be repeated in a message as digits nobody typed.
        with pytest.raises(ValueError, match="campaign_ids"):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize(
        "given",
        ["[1_2]", "[+999]", "[0x0c]", "['1_2']", "{1_2}", "[12, 1_2]"],
    )
    def test_a_written_out_container_holds_ids_to_the_same_spelling(self, given):
        # A number read out of a literal keeps its value and nothing of how it
        # was written, so the rule is asked of the text: a container is not the
        # way round the one spelling an id is written in.
        with pytest.raises(ValueError, match="campaign_ids"):
            CampaignSelection.parse(ids=given)

    def test_a_fractional_number_in_a_container_is_named_a_float(self):
        # A number written with a fractional part is no campaign id in any
        # spelling, so the refusal names its type rather than the characters it
        # was typed as.
        with pytest.raises(ValueError, match="a float of"):
            CampaignSelection.parse(ids="[1.5]")

    def test_a_number_written_out_in_a_container_is_quoted_back(self):
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids="[+999]")
        assert repr("+999") in str(raised.value)

    def test_the_refusal_says_which_spelling_is_asked_for(self):
        # A refusal that only called the value "not a positive whole number"
        # would be describing a value that is one: what is wrong with "+999" is
        # the spelling, so the message shows the spelling that is read.
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids="+999")
        message = str(raised.value)
        assert "999, and neither +999 nor 000999" in message

    @pytest.mark.parametrize("given", ["123", "  123  ", 123, "9" * 20])
    def test_an_accepted_id_written_out_is_the_text_that_was_typed(self, given):
        # What a message repeats about an id is the number written out, and a
        # masking gate recognises the text it was handed: the two are the same
        # text, so a credential typed here reaches that gate as itself.
        (parsed,) = CampaignSelection.parse(ids=given).ids
        assert str(parsed) == str(given).strip()


class TestParseNames:
    """Every spelling a name list reaches the operator in."""

    @pytest.mark.parametrize(
        "given",
        [
            ["Летняя кампания", "Зимняя"],
            ("Летняя кампания", "Зимняя"),
            {"Летняя кампания", "Зимняя"},
            "Летняя кампания,Зимняя",
            "  Летняя кампания , Зимняя ",
            "['Летняя кампания', 'Зимняя']",
            '["Летняя кампания", "Зимняя"]',
        ],
    )
    def test_it_reads_the_same_two_campaigns(self, given):
        assert CampaignSelection.parse(names=given).names == frozenset(
            {"Летняя кампания", "Зимняя"}
        )

    def test_a_rendered_jinja_list_of_one_is_read(self):
        assert CampaignSelection.parse(names="['Летняя кампания']").names == frozenset(
            {"Летняя кампания"}
        )

    @pytest.mark.parametrize("given", [None, "", "   ", []])
    def test_nothing_given_leaves_the_selection_open(self, given):
        selection = CampaignSelection.parse(names=given)
        assert selection.names == frozenset()
        assert selection.is_explicit is False

    @pytest.mark.parametrize("given", [["Лето", "", "Зима"], "['Лето', '', 'Зима']", "Лето,,Зима"])
    def test_an_empty_name_is_dropped_rather_than_kept(self, given):
        # An empty name matches no campaign and would still make the selection
        # explicit, sending the day off collecting nothing at all.
        selection = CampaignSelection.parse(names=given)
        assert selection.names == frozenset({"Лето", "Зима"})

    @pytest.mark.parametrize("given", [[""], ["   "], "['']", " , "])
    def test_a_list_of_nothing_but_blanks_is_not_an_explicit_selection(self, given):
        assert CampaignSelection.parse(names=given).is_explicit is False

    def test_a_name_is_trimmed(self):
        assert CampaignSelection.parse(names=["  Лето  "]).names == frozenset({"Лето"})

    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("[EN] Summer, [RU] Winter", {"[EN] Summer", "[RU] Winter"}),
            ("[EN] Summer", {"[EN] Summer"}),
            ("{RU} Зима", {"{RU} Зима"}),
        ],
    )
    def test_a_name_written_in_brackets_is_a_name(self, given, expected):
        # A bracket opens an ad-ops name as often as it opens a list, so text
        # that does not read as a list is read as the names it spells out.
        assert CampaignSelection.parse(names=given).names == frozenset(expected)

    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("[MSK] Лето [2026]", {"[MSK] Лето [2026]"}),
            ("[Промо] Осень]", {"[Промо] Осень]"}),
            ("[Test]", {"[Test]"}),
            ("{Лето}", {"{Лето}"}),
            ("(Лето)", {"(Лето)"}),
            (
                "[EN] Summer, [RU] Зима [2026]",
                {"[EN] Summer", "[RU] Зима [2026]"},
            ),
        ],
    )
    def test_a_name_closing_on_a_bracket_is_still_a_name(self, given, expected):
        # A cabinet tags a campaign with its locale, its market and its year in
        # brackets, so a name opens and closes on one as a matter of course —
        # and the comma-separated line is the form a run form is typed in.
        assert CampaignSelection.parse(names=given).names == frozenset(expected)

    def test_a_name_holding_a_comma_is_written_as_a_list(self):
        assert CampaignSelection.parse(names="['Лето, осень']").names == frozenset(
            {"Лето, осень"}
        )

    def test_a_written_out_set_is_read_like_a_written_out_list(self):
        assert CampaignSelection.parse(names="{'Лето', 'Зима'}").names == frozenset(
            {"Лето", "Зима"}
        )

    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("('Лето', 'Зима')", {"Лето", "Зима"}),
            ('("Лето", "Зима")', {"Лето", "Зима"}),
            ("('Лето',)", {"Лето"}),
        ],
    )
    def test_a_written_out_tuple_is_read_like_a_written_out_list(self, given, expected):
        # A rendered ``{{ params.campaigns }}`` arrives as a tuple's own repr
        # whenever the value behind it is one, and it names the same campaigns.
        assert CampaignSelection.parse(names=given).names == frozenset(expected)

    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("[EN] Summer, {RU} Winter", {"[EN] Summer", "{RU} Winter"}),
            ("[EN] Лето", {"[EN] Лето"}),
            ("{RU} Зима, {EN} Summer", {"{RU} Зима", "{EN} Summer"}),
            ("[Лето, Зима]", {"[Лето", "Зима]"}),
        ],
    )
    def test_free_text_that_opens_a_bracket_is_split_on_commas(self, given, expected):
        # Brackets around ordinary words are a name, whichever bracket the line
        # ends on: what is read as a container is a bracket opening onto a quote.
        assert CampaignSelection.parse(names=given).names == frozenset(expected)


class TestParseScope:
    """The scope is a word, and a word is read case- and space-insensitively."""

    @pytest.mark.parametrize("given", ["active", "ACTIVE", " Active ", "aCtIvE"])
    def test_active_in_any_case(self, given):
        assert CampaignSelection.parse(scope=given).scope == SCOPE_ACTIVE

    @pytest.mark.parametrize("given", ["all", "ALL", " All "])
    def test_all_in_any_case(self, given):
        assert CampaignSelection.parse(scope=given).scope == SCOPE_ALL

    def test_none_is_the_default(self):
        assert CampaignSelection.parse(scope=None).scope == SCOPE_ACTIVE


class TestIsExplicit:
    """Whether campaigns were asked for one by one."""

    def test_ids_alone_are_explicit(self):
        assert CampaignSelection.parse(ids="1").is_explicit is True

    def test_names_alone_are_explicit(self):
        assert CampaignSelection.parse(names="Лето").is_explicit is True

    def test_both_are_explicit(self):
        assert CampaignSelection.parse(ids="1", names="Лето").is_explicit is True

    def test_a_scope_alone_is_not(self):
        assert CampaignSelection.parse(scope=SCOPE_ALL).is_explicit is False


class TestRefusals:
    """A value that names no campaign stops the export where it is written."""

    @pytest.mark.parametrize("given", ["12a", 0, -1, [0], [-1], ["12a"], "0", "-1", 12.5, True])
    def test_an_id_that_is_no_id(self, given):
        with pytest.raises(ValueError, match="campaign_ids"):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize("given", ["0", "-1", "+0"])
    def test_a_number_refused_for_its_value_is_quoted_back(self, given):
        # Digits are the shape no credential is written in, and the shape a
        # length says least about: these are refused for how much they are, not
        # for how they are written.
        with pytest.raises(ValueError, match=re.escape(repr(given))):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize("given", [[5], [None], [["Лето"]]])
    def test_a_name_that_is_no_name(self, given):
        with pytest.raises(ValueError, match="campaign_names"):
            CampaignSelection.parse(names=given)

    @pytest.mark.parametrize("given", ["[1, 2", "['Лето'", "[1, 2]]"])
    def test_a_bracket_that_does_not_close(self, given):
        with pytest.raises(ValueError, match="does not read as a list"):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize(
        "given",
        [
            "['Лето', 'Зима'",
            '["Лето", "Зима"',
            "{'Лето', 'Зима'",
            "('Лето', 'Зима'",
            "['Лето', Зима]",
        ],
    )
    def test_a_container_written_wrong_is_no_name(self, given):
        # A bracket opening onto a quote is the one shape that says a container
        # was meant, and one written with a bracket or a quote missing would
        # split into names of punctuation: an explicit selection that overrides
        # the scope and then matches nothing — an empty day and a green task.
        with pytest.raises(ValueError, match="does not read as a list"):
            CampaignSelection.parse(names=given)

    def test_the_refusal_of_a_name_list_says_how_to_write_one(self):
        # The spelling that works for a name is the quoted one, since a name is
        # free text: advice about closing a bracket names nothing to fix for a
        # value whose brackets are all closed already.
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(names="['Лето', 'Зима'")
        message = str(raised.value)
        assert "every name is written in quotes" in message
        assert "one line with commas between them" in message

    def test_a_written_out_id_list_with_a_leading_zero_says_so(self):
        # Python reads no number written ``0999``, so the container never
        # parses, and the refusal has to name the zero rather than send the
        # caller off closing brackets that are closed already.
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids="[0999, 1000]")
        message = str(raised.value)
        assert "no leading zero" in message
        assert "0999 is no number to Python" in message

    def test_a_leading_zero_that_python_does_read_is_refused_as_an_id(self):
        # ``00`` is a zero to Python, so the container parses and the element
        # is refused by the rule about the one spelling an id is written in.
        with pytest.raises(ValueError, match=re.escape("neither +999 nor 000999")):
            CampaignSelection.parse(ids="[00, 1]")

    def test_the_refusal_of_an_id_list_says_how_to_write_one(self):
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids="[123, 534")
        message = str(raised.value)
        assert "an id is written as its own digits" in message
        assert "999, 1000" in message

    def test_a_container_written_wrong_names_nothing_of_itself(self):
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(names=f"['{TOKEN}'")
        assert TOKEN not in str(raised.value)

    @pytest.mark.parametrize("given", ["{[1]}", "{{}}", "{ {1} }", "{[1], 2}"])
    def test_a_set_written_over_an_element_that_cannot_be_in_one(self, given):
        # A set of a list is a literal the interpreter refuses while building
        # it, with a TypeError rather than either error a malformed literal
        # raises. Every refusal of the module is a ValueError all the same.
        with pytest.raises(ValueError, match="does not read as a list"):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize("given", ["{'Лето', [1]}", "{'Лето', {'Зима'}}"])
    def test_a_set_of_names_over_an_element_that_cannot_be_in_one(self, given):
        with pytest.raises(ValueError, match="does not read as a list"):
            CampaignSelection.parse(names=given)

    def test_a_run_of_digits_too_long_to_read_as_a_number(self):
        # CPython reads only so long a run of digits as an integer; past that
        # the refusal is still the module's own, worded about campaign ids.
        with pytest.raises(ValueError, match="is not a campaign id"):
            CampaignSelection.parse(ids="9" * 5000)

    @pytest.mark.parametrize("given", [10**5000, -(10**5000)], ids=["above", "below"])
    def test_an_integer_too_long_to_write_out(self, given):
        # An integer handed over as one reaches no ``int()`` call, and CPython
        # renders no digits of it either. Both refusals are the module's own,
        # and neither says a word about the interpreter's limit.
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids=[given])
        message = str(raised.value)
        assert "is not a campaign id" in message
        assert "more digits than can be written" in message
        assert "int_max_str_digits" not in message

    @pytest.mark.parametrize("given", ["{'a': 1}", '{"a": 1}'])
    def test_a_mapping_where_a_list_belongs(self, given):
        with pytest.raises(ValueError, match="reads as a dict"):
            CampaignSelection.parse(names=given)

    @pytest.mark.parametrize("given", ["activ", "", "  ", "none", "ALLE", 1, True, ["all"]])
    def test_a_scope_that_is_neither_word(self, given):
        with pytest.raises(ValueError, match="campaign_scope"):
            CampaignSelection.parse(scope=given)

    def test_the_two_words_are_offered_back(self):
        with pytest.raises(ValueError, match='"active" or "all"'):
            CampaignSelection.parse(scope="activ")


class TestARefusalQuotesNoSecret:
    """Parsing runs before a token is read, so only a short id is quoted back."""

    def test_a_long_id_value_is_named_by_type_and_length(self):
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids=TOKEN)
        message = str(raised.value)
        assert TOKEN not in message
        assert f"a str of {len(TOKEN)} character(s)" in message

    def test_a_long_scope_value_is_named_by_type_and_length(self):
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(scope=TOKEN)
        assert TOKEN not in str(raised.value)

    def test_a_long_name_value_is_named_by_type_and_length(self):
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(names=[TOKEN.encode()])
        message = str(raised.value)
        assert TOKEN not in message
        assert "a value of type bytes" in message

    @pytest.mark.parametrize("given", ["a-b!", "\u043b\u0435\u0442\u043e", "12 34", "a b", "12a"])
    def test_a_short_value_that_is_not_a_number_is_described_by_length(self, given):
        # Only a run of ASCII digits is quoted: everything else is a shape a
        # credential can have, and a length says enough about a typo.
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids=given)
        message = str(raised.value)
        assert given not in message
        assert f"a str of {len(given)} character(s)" in message

    @pytest.mark.parametrize("given", ["0" * 8, "-1234567", "+0"])
    def test_a_number_within_the_bound_is_quoted_back(self, given):
        with pytest.raises(ValueError, match=re.escape(repr(given))):
            CampaignSelection.parse(ids=given)

    @pytest.mark.parametrize("given", ["0" * 9, "-12345678", "-" + "9" * 100])
    def test_a_number_past_the_bound_travels_as_its_length(self, given):
        # The bound is what stands between a campaign id and a run of digits
        # long enough to be an account number or a card: one character over it
        # and the value stays out of the message. It is the written form that is
        # measured, so a sign counts towards the length like a digit.
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids=given)
        message = str(raised.value)
        assert given not in message
        assert f"a str of {len(given)} character(s)" in message

    def test_a_number_given_as_one_is_bounded_by_the_same_length(self):
        # A scalar is held to the bound its written form has, so a thousand-digit
        # integer is described rather than printed.
        given = -(10**400)
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids=given)
        message = str(raised.value)
        assert str(given) not in message
        assert f"a int of {len(str(given))} character(s)" in message

    def test_digits_are_not_quoted_where_a_number_names_no_campaign(self):
        # A scope is a word and a name is text: digits there diagnose nothing
        # worth repeating, and a numeric PIN is a run of digits too.
        pin = "123456"
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(scope=pin)
        assert pin not in str(raised.value)
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(names=[int(pin)])
        assert pin not in str(raised.value)

    @pytest.mark.parametrize("parameter", ["ids", "scope"])
    def test_a_short_credential_is_never_quoted_back(self, parameter):
        # A password typed into the wrong field is short and made of letters and
        # digits; the refusal names the parameter and keeps the value out of the
        # task log and the traceback alike.
        secret = "hunter2"
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(**{parameter: secret})
        assert secret not in str(raised.value)

    def test_a_broken_bracket_leaves_no_printable_trace_of_its_value(self):
        # ``SyntaxError`` keeps the offending text in its ``text`` attribute and
        # a traceback prints it in full, so the chain is broken at the raise.
        broken = f"['{TOKEN}'"
        with pytest.raises(ValueError) as raised:
            CampaignSelection.parse(ids=broken)
        error = raised.value
        printed = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        assert TOKEN not in printed
        assert error.__cause__ is None
        assert error.__suppress_context__ is True
        # ``from None`` silences the printing of the context, it does not clear
        # it, and the printing is the channel this test is about.
        assert isinstance(error.__context__, SyntaxError)


def _campaign(campaign_id, name, status="active", **extra):
    """One list entry, worded the way the management API words it."""
    return {"campaign_id": campaign_id, "name": name, "status": status, **extra}


def _listed(campaign_id, name, status="active", **extra):
    """One entry of the cabinet every test of this module shares.

    ``matching`` hands the entries it kept straight back to its caller, so a
    shared entry is one a test could write into and leave altered for every test
    declared after it.  A read-only view answers every read a selection makes
    and refuses the write.
    """
    return MappingProxyType(_campaign(campaign_id, name, status, **extra))


SUMMER = _listed(1, "Summer")
WINTER = _listed(2, "Winter", status="archived")
SPRING = _listed(3, "Spring")
CABINET = [SUMMER, WINTER, SPRING]


class TestMatchingByScope:
    """What a scope walks when no campaign was named one by one."""

    def test_active_keeps_the_running_campaigns(self):
        assert CampaignSelection().matching(CABINET) == [SUMMER, SPRING]

    def test_all_keeps_the_whole_list(self):
        selection = CampaignSelection(scope=SCOPE_ALL)
        assert selection.matching(CABINET) == CABINET

    def test_all_reads_no_status_at_all(self):
        nameless_status = [{"campaign_id": 7, "name": "Seven"}]
        selection = CampaignSelection(scope=SCOPE_ALL)
        assert selection.matching(nameless_status) == nameless_status

    @pytest.mark.parametrize("status", ["ACTIVE", " active ", "Active"])
    def test_a_status_is_read_past_case_and_space(self, status):
        campaigns = [_campaign(9, "Nine", status=status)]
        assert CampaignSelection().matching(campaigns) == campaigns

    @pytest.mark.parametrize(
        "campaign",
        [
            {"campaign_id": 9, "name": "Nine"},
            _campaign(9, "Nine", status=None),
            _campaign(9, "Nine", status=7),
        ],
    )
    def test_a_campaign_without_a_readable_status_is_not_active(self, campaign):
        assert CampaignSelection().matching([campaign]) == []

    def test_an_empty_list_matches_nothing(self):
        assert CampaignSelection().matching([]) == []
        assert CampaignSelection(scope=SCOPE_ALL).matching([]) == []

    def test_the_order_of_the_cabinet_survives(self):
        listed = [SPRING, SUMMER]
        assert CampaignSelection().matching(listed) == [SPRING, SUMMER]


class TestMatchingByExplicitSelection:
    """What naming campaigns one by one walks, and what it ignores."""

    def test_by_id(self):
        selection = CampaignSelection.parse(ids=[1, 3])
        assert selection.matching(CABINET) == [SUMMER, SPRING]

    def test_by_name(self):
        selection = CampaignSelection.parse(names="Summer")
        assert selection.matching(CABINET) == [SUMMER]

    def test_ids_and_names_join_by_or(self):
        selection = CampaignSelection.parse(ids=[3], names="Summer")
        assert selection.matching(CABINET) == [SUMMER, SPRING]

    def test_an_archived_campaign_named_by_id_overrides_the_scope(self):
        selection = CampaignSelection.parse(scope=SCOPE_ACTIVE, ids=[2])
        assert selection.matching(CABINET) == [WINTER]

    def test_an_archived_campaign_named_by_name_overrides_the_scope(self):
        selection = CampaignSelection.parse(scope=SCOPE_ACTIVE, names="Winter")
        assert selection.matching(CABINET) == [WINTER]

    def test_a_campaign_named_twice_over_is_walked_once(self):
        selection = CampaignSelection.parse(ids=[1], names="Summer")
        assert selection.matching(CABINET) == [SUMMER]

    def test_one_name_may_match_several_campaigns(self):
        twins = [_campaign(1, "Summer"), _campaign(2, "Summer", status="archived")]
        selection = CampaignSelection.parse(names="Summer")
        assert selection.matching(twins) == twins

    def test_the_order_of_the_cabinet_survives(self):
        selection = CampaignSelection.parse(ids=[3, 1])
        assert selection.matching(CABINET) == [SUMMER, SPRING]

    @pytest.mark.parametrize("listed", ["  Summer", "Summer  ", " Summer "])
    def test_space_around_a_name_is_dropped_on_the_cabinet_side(self, listed):
        campaigns = [_campaign(5, listed)]
        assert CampaignSelection.parse(names="Summer").matching(campaigns) == campaigns

    def test_space_around_a_name_is_dropped_on_the_selection_side(self):
        selection = CampaignSelection(names=frozenset({"  Summer  "}))
        assert selection.matching(CABINET) == [SUMMER]

    def test_a_name_of_another_case_is_another_name(self):
        assert CampaignSelection.parse(names="SUMMER").matching(CABINET) == []

    @pytest.mark.parametrize(
        "campaign",
        [
            {"campaign_id": 9, "status": "active"},
            _campaign(9, None),
            _campaign(9, 42),
        ],
    )
    def test_a_campaign_without_a_readable_name_matches_no_name(self, campaign):
        assert CampaignSelection.parse(names="Summer").matching([campaign]) == []

    def test_a_campaign_without_a_readable_id_matches_no_id(self):
        campaigns = [{"campaign_id": "1", "name": "Summer", "status": "active"}]
        assert CampaignSelection.parse(ids=[1]).matching(campaigns) == []

    def test_an_explicit_selection_ignores_the_scope_all_too(self):
        selection = CampaignSelection.parse(scope=SCOPE_ALL, ids=[1])
        assert selection.matching(CABINET) == [SUMMER]


class TestPartition:
    """The campaigns a day walks and the ones it leaves, answered together."""

    def test_a_scope_splits_the_cabinet_in_two(self):
        assert CampaignSelection().partition(CABINET) == ([SUMMER, SPRING], [WINTER])

    def test_the_walked_half_is_what_matching_says(self):
        selection = CampaignSelection.parse(ids=[2], names="Spring")
        walked, skipped = selection.partition(CABINET)
        assert walked == selection.matching(CABINET)
        assert skipped == [SUMMER]

    def test_every_campaign_lands_in_exactly_one_half(self):
        walked, skipped = CampaignSelection().partition(CABINET)
        assert len(walked) + len(skipped) == len(CABINET)

    def test_a_repeated_campaign_is_split_and_not_deduplicated(self):
        twins = [SUMMER, SUMMER, WINTER]
        assert CampaignSelection().partition(twins) == ([SUMMER, SUMMER], [WINTER])

    def test_the_all_scope_skips_nothing(self):
        selection = CampaignSelection(scope=SCOPE_ALL)
        assert selection.partition(CABINET) == (CABINET, [])

    def test_an_empty_list_splits_into_two_empty_halves(self):
        assert CampaignSelection().partition([]) == ([], [])


class TestMissing:
    """What an explicit selection named that the cabinet does not list."""

    def test_nothing_is_missing_when_everything_is_listed(self):
        selection = CampaignSelection.parse(ids=[1, 2], names="Spring")
        assert selection.missing(CABINET) == (frozenset(), frozenset())

    def test_part_of_a_selection_is_missing(self):
        selection = CampaignSelection.parse(ids=[1, 99], names=["Spring", "Autumn"])
        assert selection.missing(CABINET) == (frozenset({99}), frozenset({"Autumn"}))

    def test_all_of_a_selection_is_missing(self):
        selection = CampaignSelection.parse(ids=[98, 99], names="Autumn")
        assert selection.missing(CABINET) == (
            frozenset({98, 99}),
            frozenset({"Autumn"}),
        )

    def test_a_scope_alone_misses_nothing(self):
        assert CampaignSelection().missing(CABINET) == (frozenset(), frozenset())
        assert CampaignSelection().missing([]) == (frozenset(), frozenset())

    def test_a_campaign_matched_past_surrounding_space_is_not_missing(self):
        campaigns = [_campaign(5, "  Summer  ")]
        selection = CampaignSelection.parse(names="Summer")
        assert selection.matching(campaigns) == campaigns
        assert selection.missing(campaigns) == (frozenset(), frozenset())

    def test_a_name_of_another_case_is_missing(self):
        selection = CampaignSelection.parse(names="SUMMER")
        assert selection.matching(CABINET) == []
        assert selection.missing(CABINET) == (frozenset(), frozenset({"SUMMER"}))

    @pytest.mark.parametrize(
        "campaign",
        [{"campaign_id": 9, "status": "active"}, _campaign(9, None)],
    )
    def test_a_campaign_without_a_readable_name_names_nothing(self, campaign):
        selection = CampaignSelection.parse(names="Summer")
        assert selection.missing([campaign]) == (frozenset(), frozenset({"Summer"}))

    def test_an_id_of_another_type_is_not_the_id_asked_for(self):
        campaigns = [{"campaign_id": "1", "name": "Summer", "status": "active"}]
        assert CampaignSelection.parse(ids=[1]).missing(campaigns) == (
            frozenset({1}),
            frozenset(),
        )
