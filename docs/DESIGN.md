# Frontend design system (provided by the team): "Premium Minimal Fintech"

Mercury-style: premium, minimal, trustworthy, calm, editorial, spacious.

Colours
- primary_background #E8E7F5, secondary_background #D9D9EA, card_background #F5F5FA, white #FFFFFF
- dark_section #303A56, dark_section_secondary #414B68 (dark navy blocks for contrast; text #FFFFFF, muted #C4C8D6, border rgba(255,255,255,0.12))
- primary_text #202943, secondary_text #66708A, muted_text #858DA3, dark_text #172038
- primary_accent #5B68D8, accent_hover #4D5AC8, accent_light #C9CDF0
- border #D1D3E1, border_dark #566079
- success #6E8F82, warning #C7A86B, error #B86D72

Typography: font-family Inter, "SF Pro Display", -apple-system, BlinkMacSystemFont, sans-serif (system fonts only, no web-font download)
- headings weight 500, letter-spacing -0.04em, line-height 1.05
- hero heading clamp(48px,5vw,76px) / 500 / 0.98 / -0.055em; section heading clamp(36px,4vw,58px) / 500 / 1.02 / -0.045em
- body 14px / 1.5; body_large 18px; small 12px / 1.4; nav 13px / 500 / -0.01em

Layout: max width 1440px, content 1180px, padding 32px, grid gap 24px; radius small 8, medium 14, large 24, xl 32, pill 999.
Navigation: 72px high, transparent; logo 18px/600/0.04em letter-spacing; links #30384F hover #5B68D8; CTA pill #5B68D8 white 12px, padding 10px 18px.
Buttons: primary pill #5B68D8 (hover #4D5AC8) white 500; secondary transparent, 1px #AEB3C7, #30384F, pill; text button #5B68D8.
Cards: background #F5F5FA, border 1px rgba(80,88,120,0.08), radius 20px, padding 24px, shadow 0 10px 40px rgba(40,45,75,0.06); hover translateY(-3px), shadow 0 16px 50px rgba(40,45,75,0.10), 250ms ease.
Forms: input white, 1px #D1D3E1, focus #5B68D8, radius 8px, height 44px, placeholder #9298AA.
Statistics (on dark sections): number 42px/400/#FFFFFF, label 12px #C4C8D6, horizontal with subtle vertical dividers.
Background graphics: very subtle concentric curves, white, opacity 0.12, stroke 1, on dark hero/CTA sections.
Icons: thin line, stroke 1.5, #59627C, 18px.
Shadows: subtle 0 4px 20px rgba(30,35,60,0.05); card 0 10px 40px rgba(30,35,60,0.07); floating 0 20px 60px rgba(30,35,60,0.12).
Spacing: 4 / 8 / 16 / 24 / 40 / 64 / 120.
Responsive: desktop >=1200 two columns; tablet 768 stacked or two; mobile 640 single column, 20px padding.

Rules: generous whitespace; elegant, relatively thin type; muted lavender backgrounds instead of pure white; dark navy sections for contrast;
rounded but not excessive; very subtle shadows; no heavy gradients, no neon, no glassmorphism, avoid dense dashboards; large editorial
headlines; asymmetrical layouts where appropriate; quiet premium UI; strong alignment; subtle animations (fade, slide-up, small hover lift).
States must still use colour + icon + text (never colour alone): HEALTHY/VERIFIED = success, DEGRADED/USE CAUTION = warning, CRITICAL/DO NOT USE = error.
