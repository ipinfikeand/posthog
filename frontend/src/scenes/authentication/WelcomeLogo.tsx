import { useValues } from 'kea'

import { Link } from '@posthog/lemon-ui'

import { preflightLogic } from 'scenes/PreflightCheck/preflightLogic'

import demoLogo from 'public/posthog-logo-demo.svg'
import defaultLogo from 'public/posthog-logo.svg'

export function WelcomeLogo({ view }: { view?: string }): JSX.Element {
    const UTM_TAGS = `utm_campaign=in-product&utm_tag=${view || 'welcome'}-header`
    const { preflight } = useValues(preflightLogic)

    const logoSrc = preflight?.demo ? demoLogo : defaultLogo
    const altText = `PostHog${preflight?.cloud ? ' Cloud' : ''}`
    const logoHref = `https://posthog.com?${UTM_TAGS}`

    return (
        <Link to={logoHref} className="flex flex-col items-center mb-8">
            <span className="flex items-center gap-2">
                <img src={logoSrc} alt={altText} className="h-6" />
                {preflight?.cloud && !preflight?.demo && (
                    <span className="text-primary text-xl font-bold leading-none">Cloud</span>
                )}
            </span>
        </Link>
    )
}
