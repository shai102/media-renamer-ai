import unittest

from ai.ollama_ai import _extract_siliconflow_content
from core.services.matcher_service import extract_ollama_model_names
from core.services.naming_service import (
    can_reuse_dir_ai,
    extract_explicit_season,
    pick_season,
)
from core.services.template_service import (
    build_filename_context,
    render_filename_template,
)
from core.workers.task_runner import (
    SPECIAL_TAG_RE,
    _guessit_needs_assist,
    _is_meaningful_title,
)
from utils.helpers import (
    build_query_titles,
    format_error_message,
    parse_error_message,
    safe_filename,
)


class SmokeTests(unittest.TestCase):
    def test_safe_filename_replaces_illegal_chars(self):
        original = 'a<b>:"c/\\d|?*.'
        self.assertEqual(safe_filename(original), "a_b___c__d___")

    def test_extract_siliconflow_content_success(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "content": '{"title":"Test","year":2024,"season":1,"episode":1}'
                    }
                }
            ]
        }
        self.assertEqual(
            _extract_siliconflow_content(payload),
            '{"title":"Test","year":2024,"season":1,"episode":1}',
        )

    def test_extract_siliconflow_content_rejects_invalid_shape(self):
        with self.assertRaises(ValueError):
            _extract_siliconflow_content({"choices": []})

    def test_extract_ollama_model_names_success(self):
        payload = {
            "models": [
                {"name": "qwen2.5:14b"},
                {"name": "nomic-embed-text:latest"},
                {"name": "qwen2.5:14b"},
            ]
        }
        self.assertEqual(
            extract_ollama_model_names(payload),
            ["qwen2.5:14b", "nomic-embed-text:latest"],
        )

    def test_extract_ollama_model_names_rejects_invalid_shape(self):
        with self.assertRaises(ValueError):
            extract_ollama_model_names({"models": "bad"})

    def test_error_message_format_and_parse(self):
        msg = format_error_message("timeout", "请求超时")
        self.assertEqual(msg, "TIMEOUT:请求超时")
        self.assertEqual(parse_error_message(msg), ("TIMEOUT", "请求超时"))

    def test_error_message_parse_legacy_text(self):
        self.assertEqual(parse_error_message("未配置TMDb Key")[0], "CONFIG")

    def test_build_query_titles_filters_generic_season_title(self):
        item = {
            "old_name": "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC.DDP-Xiaomi.strm",
            "dir": r"D:\Media\Season 1",
        }
        g = {"title": "Extracurricular"}
        titles = build_query_titles(item, "Season 1", None, g)
        self.assertIn("Extracurricular", titles)
        self.assertNotIn("Season 1", titles)

    def test_build_query_titles_keeps_real_title(self):
        item = {
            "old_name": "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC.DDP-Xiaomi.strm",
            "dir": r"D:\Media\Season 1",
        }
        g = {"title": "Extracurricular"}
        titles = build_query_titles(item, "Extracurricular", None, g)
        self.assertIn("Extracurricular", titles)

    def test_extract_explicit_season_from_sxxeyy(self):
        name = "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC.strm"
        self.assertEqual(extract_explicit_season(name), 1)

    def test_pick_season_ignores_zero_fallback(self):
        season = pick_season("Extracurricular.E01.2020", {}, 0)
        self.assertEqual(season, 1)

    def test_pick_season_uses_explicit_over_zero_guess(self):
        season = pick_season("Extracurricular.S01E01.2020", {"season": 0}, 0)
        self.assertEqual(season, 1)

    def test_special_tag_regex_does_not_match_extracurricular(self):
        name = "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC"
        self.assertIsNone(SPECIAL_TAG_RE.search(name))

    def test_special_tag_regex_matches_real_special_marker(self):
        name = "Anime.Title.S01E01.[NC.Ver].1080p"
        self.assertIsNotNone(SPECIAL_TAG_RE.search(name))

    def test_guessit_assist_detects_group_release_style(self):
        g = {"title": "Dungeon Meshi"}
        self.assertTrue(
            _guessit_needs_assist(
                "[KTXP][Dungeon Meshi][01][CHS][1080P][AVC]",
                r"D:\Anime\Dungeon Meshi",
                g,
                "Dungeon Meshi",
                1,
            )
        )

    def test_guessit_assist_skips_clean_standard_name(self):
        g = {"title": "The Mandalorian", "episode": 4}
        self.assertFalse(
            _guessit_needs_assist(
                "The.Mandalorian.S03E04.2023.WEB-DL",
                r"D:\TV\The Mandalorian\Season 3",
                g,
                "The Mandalorian",
                4,
            )
        )

    def test_guessit_assist_skips_clean_standard_name_in_localized_season_dir(self):
        g = {
            "title": "Frieren Beyond Journeys End",
            "season": 1,
            "episode": 1,
            "type": "episode",
        }
        self.assertFalse(
            _guessit_needs_assist(
                "Frieren.Beyond.Journeys.End.S01E01.2023.1080p.BluRay.Remux",
                r"Y:\STRM\动漫刮削好的\葬送的芙莉莲（2023）\Season 1",
                g,
                "Frieren Beyond Journeys End",
                1,
            )
        )

    def test_is_meaningful_title_rejects_generic_values(self):
        self.assertFalse(_is_meaningful_title("未知"))
        self.assertFalse(_is_meaningful_title("Season 1"))
        self.assertTrue(_is_meaningful_title("Violet Evergarden"))

    def test_can_reuse_dir_ai_accepts_cached_alias_title(self):
        cached_ai = {
            "title": "葬送的芙莉莲",
            "title_aliases": ["Frieren Beyond Journeys End"],
            "year": 2023,
        }
        guess_data = {"title": "Frieren Beyond Journeys End", "year": 2023}
        self.assertTrue(
            can_reuse_dir_ai(
                cached_ai,
                "Frieren.Beyond.Journeys.End.S01E02.2023.1080p.BluRay.Remux",
                guess_data,
            )
        )

    def test_render_filename_template_legacy_still_works(self):
        context = build_filename_context(
            title="剑来",
            season="01",
            episode="02",
            ep_name="天涯咫尺",
            ext=".strm",
            media_suffix="2160p.TVING.WEB-DL.H.265.AAC-ColorTV",
            source_provider="tmdb",
            media_id="259537",
            is_tv=True,
        )
        rendered = render_filename_template(
            "{title} - S{s:02d}E{e:02d} - {ep_name}{ext}",
            context,
            preserve_media_suffix=True,
        )
        self.assertEqual(
            rendered,
            "剑来 - S01E02 - 天涯咫尺 - 2160p.TVING.WEB-DL.H.265.AAC-ColorTV.strm",
        )

    def test_render_filename_template_advanced_jinja(self):
        context = build_filename_context(
            title="信号",
            year="2016",
            season="01",
            episode="01",
            ep_name="回响",
            ext=".strm",
            media_suffix="2160p.TVING.WEB-DL.H.265.AAC-ColorTV",
            parse_source="guessit",
            source_provider="tmdb",
            media_id="62085",
            is_tv=True,
        )
        rendered = render_filename_template(
            "{{ title }} - S{{ season }}E{{ episode }}{% if ep_name %} - {{ ep_name }}{% endif %}{% if media_suffix %} - {{ media_suffix }}{% endif %}{{ ext }}",
            context,
            preserve_media_suffix=True,
        )
        self.assertEqual(
            rendered,
            "信号 - S01E01 - 回响 - 2160p.TVING.WEB-DL.H.265.AAC-ColorTV.strm",
        )


if __name__ == "__main__":
    unittest.main()
