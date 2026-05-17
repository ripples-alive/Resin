import * as countries from "i18n-iso-countries";
import enLocale from "i18n-iso-countries/langs/en.json";
import zhLocale from "i18n-iso-countries/langs/zh.json";
import { getCurrentLocale, isEnglishLocale } from "../../i18n/locale";

export const GLOBAL_REGION_CODE = "global";
export const GLOBAL_REGION_FILTER_VALUE = GLOBAL_REGION_CODE;
export const GLOBAL_REGION_COMPACT_LABEL = "GL";

countries.registerLocale(enLocale);
countries.registerLocale(zhLocale);

export interface RegionOption {
    code: string;
    name: string;
}

function getCountryLocale(): "zh" | "en" {
    return isEnglishLocale(getCurrentLocale()) ? "en" : "zh";
}

export const getAllRegions = (): RegionOption[] => {
    const names = countries.getNames(getCountryLocale(), { select: "official" });
    const countryRegions = Object.entries(names).map(([code, name]) => ({
        code,
        name: `${code} (${name})`,
    })).sort((a, b) => a.code.localeCompare(b.code));

    return [
        {
            code: GLOBAL_REGION_FILTER_VALUE,
            name: `${GLOBAL_REGION_COMPACT_LABEL} (${isEnglishLocale(getCurrentLocale()) ? "Global" : "全球"})`,
        },
        ...countryRegions,
    ];
};

export const getRegionName = (code: string): string | undefined => {
    if (code.trim().toLowerCase() === GLOBAL_REGION_CODE) {
        return isEnglishLocale(getCurrentLocale()) ? "Global" : "全球";
    }
    return countries.getName(code, getCountryLocale(), { select: "official" });
};

export const getCompactRegionLabel = (region: string | undefined): string => {
    const normalized = region?.trim();
    if (!normalized) {
        return "-";
    }

    if (normalized.toLowerCase() === GLOBAL_REGION_CODE) {
        return GLOBAL_REGION_COMPACT_LABEL;
    }

    if (normalized.length !== 2) {
        return normalized;
    }

    const code = normalized.toUpperCase();
    const flag = String.fromCodePoint(...[...code].map((c) => c.charCodeAt(0) + 127397));
    return `${flag} ${code}`;
};

export const getRegionTitle = (region: string | undefined): string => {
    const label = getCompactRegionLabel(region);
    const normalized = region?.trim();
    if (!normalized || normalized.length !== 2) {
        const name = normalized ? getRegionName(normalized) : undefined;
        return name ? `${label} (${name})` : label;
    }

    const code = normalized.toUpperCase();
    const name = getRegionName(code);
    return name ? `${label} (${name})` : label;
};
