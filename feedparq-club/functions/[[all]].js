const slugsFile = require('./slugs.json');

const ASSETS_BASE_URL = new URL('https://assets.feedparq.club');
const EMAIL_ADDRESS = 'feedparq@proton.me';
const RESERVED_SLUGS = new Set(['/wtf']);

const SITE_SLUGS = Object.assign({}, slugsFile, {
  '/distributed-systems': {
    curated: {
      link: 'https://discord.gg/jcphEVmxxc',
      text: 'feedparq'
    },
  },
  '/scottish-culture': {
    curated: {
      link: 'https://x.com/scotswhayhae',
      text: '@scotswhayhae',
    },
    sponsored: {
      link: 'https://www.scotswhayhae.com',
      text: 'scotswhayhae.com',
    },
  },
  '/news': {
    curated: {
      link: 'https://discord.gg/S7V93hbbSh',
      text: 'feedparq',
    },
  },
});

const SITE_SLUGS_IDX = Object.keys(SITE_SLUGS);

export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);

  // redirect home page to about for now
  if (
    !url.pathname
    || url.pathname === '/'
    || url.pathname === ''
  ) {
    url.pathname = '/wtf';
    return Response.redirect(url, 301);
  }

  // redirect to a random slug
  if (url.pathname === '/random') {
    const randomIdx = Math.floor(Math.random() * 1e6) % SITE_SLUGS_IDX.length;
    const randomSlug = SITE_SLUGS_IDX[randomIdx];
    const newUrl = new URL(randomSlug, url.origin);
    console.log(newUrl)
    return Response.redirect(newUrl, 302);
  }

  // else assume static assets
  const response = await env.ASSETS.fetch(request);

  // if we're got a slug, rewrite the path to the feeds
  const slug = url.pathname;
  if (!RESERVED_SLUGS.has(slug)) {
    return new HTMLRewriter()
      .on('link[data-channel]', {
        element(element) {
          const channelUrl = new URL(`feeds${slug}.parquet`, ASSETS_BASE_URL);
          element.setAttribute('href', channelUrl.href);
        }
      })
      .on('nav > a#next', {
        element(element) {
          const currentSlugIdx = SITE_SLUGS_IDX.findIndex(slug => url.pathname === slug);
          const nextSlugIdx = (currentSlugIdx + 1) % SITE_SLUGS_IDX.length;
          const nextSlug = SITE_SLUGS_IDX[nextSlugIdx];
          const nextUrl = new URL(nextSlug, url.origin);
          element.setAttribute('title', nextSlug);
          element.setAttribute('href', nextUrl.href);
        }
      })
      .on('a#sponsored', {
        element(element) {
          const slug = url.pathname;
          if (slug in SITE_SLUGS) {
            const { sponsored } = SITE_SLUGS[slug];
            if (!sponsored) {
              // element.setAttribute('href', `mailto:${EMAIL_ADDRESS}`);
              // element.setInnerContent('no one - sponsor it?');
              element.remove();
              return;
            }
            element.setAttribute('href', sponsored.link);
            element.setInnerContent(sponsored.text);
          }
        }
      })
      .on('a#curated', {
        element(element) {
          const slug = url.pathname;
          if (slug in SITE_SLUGS) {
            const { curated } = SITE_SLUGS[slug];
            if (!curated) {
              // element.setAttribute('href', `mailto:${EMAIL_ADDRESS}`);
              // element.setInnerContent('no one - curate it?');
              element.remove();
              return;
            }
            element.setAttribute('href', curated.link);
            element.setInnerContent(curated.text);
          }
        }
      })
      .transform(response);
  }

  return response;
}
