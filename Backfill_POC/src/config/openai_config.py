import os


class OpenAIConfig:

    @staticmethod
    def get_api_key():

        api_key = os.getenv(
            "OPENAI_API_KEY"
        )

        if not api_key:

            raise ValueError(
                "OPENAI_API_KEY is not configured."
            )

        return api_key