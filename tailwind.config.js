/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        "./templates/**/*.html",
    ],
    theme: {
        extend: {
            colors: {
                'co-green': '#8cbe69',
                'co-purple': {
                    DEFAULT: '#6d4488',
                    50:  '#f5f0f8',
                    100: '#ebe1f1',
                    600: '#6d4488',
                    700: '#5a3870',
                    800: '#472c59',
                },
            },
        },
    },
}
