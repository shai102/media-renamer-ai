import re

from jinja2 import Environment, StrictUndefined, TemplateError

from core.services.naming_service import apply_media_suffix_template
from utils.helpers import safe_str


_JINJA_ENV = Environment(
    autoescape=False,
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=False,
    undefined=StrictUndefined,
)

_JINJA_EXT_RE = re.compile(r"{{\s*ext\s*}}")
_JINJA_EXT_WITH_SEPARATOR_RE = re.compile(r"(\s*-\s*)?{{\s*ext\s*}}")
_MEDIA_SUFFIX_PLACEHOLDER_RE = re.compile(
    r"\{media_suffix\}|\{\{\s*media_suffix\s*\}\}"
)


def is_advanced_template(template):
    """Return whether a template uses Jinja-style advanced syntax."""
    text = str(template or "")
    return any(token in text for token in ("{{", "{%", "{#"))


def build_filename_context(
    *,
    title="",
    year="",
    season="",
    episode="",
    ep_name="",
    ext="",
    media_suffix="",
    parse_source="",
    source_provider="",
    media_id="",
    is_tv=True,
):
    """Build a unified context for both legacy and advanced file templates."""
    season_text = safe_str(season)
    episode_text = safe_str(episode)
    year_text = safe_str(year)
    provider_text = safe_str(source_provider).lower()
    media_id_text = safe_str(media_id)

    tmdbid = media_id_text if provider_text == "tmdb" else ""
    bgmid = media_id_text if provider_text == "bgm" else ""

    return {
        "title": safe_str(title),
        "year": year_text,
        "season": season_text,
        "episode": episode_text,
        "season_padded": season_text,
        "episode_padded": episode_text,
        "s": season_text,
        "e": episode_text,
        "ep_name": safe_str(ep_name),
        "ext": safe_str(ext),
        "media_suffix": safe_str(media_suffix),
        "parse_source": safe_str(parse_source),
        "source_provider": provider_text,
        "provider": provider_text,
        "media_id": media_id_text,
        "tmdbid": tmdbid,
        "bgmid": bgmid,
        "is_tv": bool(is_tv),
    }


def cleanup_rendered_filename(text):
    """Normalize rendered filename text and remove empty separator fragments."""
    cleaned = str(text or "")
    cleaned = re.sub(r"\s*[\(\[]\s*[\)\]]", "", cleaned)
    cleaned = re.sub(r"\s*\{\s*\}", "", cleaned)
    cleaned = re.sub(r"\s*\(\s*\)", "", cleaned)
    cleaned = re.sub(r"\s+-\s*-\s+", " - ", cleaned)
    cleaned = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", cleaned)
    cleaned = re.sub(r"\s+(?=\.)", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    return cleaned.strip()


def _inject_media_suffix_advanced(template, media_suffix, preserve_media_suffix):
    """Auto-append media suffix for advanced templates when omitted."""
    working = str(template or "")
    suffix = str(media_suffix or "").strip()
    if (
        not preserve_media_suffix
        or not suffix
        or _MEDIA_SUFFIX_PLACEHOLDER_RE.search(working)
    ):
        return working

    match = _JINJA_EXT_WITH_SEPARATOR_RE.search(working)
    if match:
        separator = match.group(1) or " - "
        replacement = f"{separator}{{{{ media_suffix }}}}{{{{ ext }}}}"
        return _JINJA_EXT_WITH_SEPARATOR_RE.sub(replacement, working, count=1)
    if re.search(r"\s*-\s*$", working):
        return working + "{{ media_suffix }}"
    return working + " - {{ media_suffix }}"


def _render_legacy_template(template, context, media_suffix, preserve_media_suffix):
    """Render old placeholder-style filename templates."""
    working = apply_media_suffix_template(template, media_suffix, preserve_media_suffix)
    rendered = (
        str(working)
        .replace("{title}", context["title"])
        .replace("{year}", context["year"])
        .replace("{s:02d}", context["season_padded"])
        .replace("{s}", context["s"])
        .replace("{e:02d}", context["episode_padded"])
        .replace("{e}", context["e"])
        .replace("{ep_name}", context["ep_name"])
        .replace("{media_suffix}", context["media_suffix"])
        .replace("{ext}", context["ext"])
    )
    return cleanup_rendered_filename(rendered)


def _render_advanced_template(template, context, media_suffix, preserve_media_suffix):
    """Render Jinja-style filename templates."""
    working = _inject_media_suffix_advanced(template, media_suffix, preserve_media_suffix)
    try:
        rendered = _JINJA_ENV.from_string(str(working)).render(**context)
    except TemplateError as err:
        raise ValueError(f"模板语法错误: {err}") from err
    return cleanup_rendered_filename(rendered)


def render_filename_template(template, context, preserve_media_suffix=False):
    """Render either a legacy or advanced filename template."""
    media_suffix = str((context or {}).get("media_suffix") or "").strip()
    if is_advanced_template(template):
        return _render_advanced_template(
            template, context, media_suffix, preserve_media_suffix
        )
    return _render_legacy_template(
        template, context, media_suffix, preserve_media_suffix
    )
