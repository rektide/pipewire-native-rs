// SPDX-License-Identifier: MIT

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#include <spa/node/command.h>
#include <spa/node/io.h>
#include <spa/param/audio/raw-utils.h>
#include <spa/pod/builder.h>

static void print_pod(const char *name, const struct spa_pod *pod)
{
	const uint8_t *bytes = (const uint8_t *)pod;
	size_t i;

	printf("%s ", name);
	for (i = 0; i < SPA_POD_SIZE(pod); i++)
		printf("%02" PRIx8, bytes[i]);
	putchar('\n');
}

static const struct spa_pod *build_format(struct spa_pod_builder *builder)
{
	const struct spa_audio_info_raw info = {
		.format = SPA_AUDIO_FORMAT_S16_LE,
		.rate = 48000,
		.channels = 2,
		.position = { SPA_AUDIO_CHANNEL_FL, SPA_AUDIO_CHANNEL_FR },
	};

	return spa_format_audio_raw_build(builder, SPA_PARAM_Format, &info);
}

int main(void)
{
	uint8_t format_buffer[1024];
	struct spa_pod_builder format_builder =
		SPA_POD_BUILDER_INIT(format_buffer, sizeof(format_buffer));
	const struct spa_pod *format = build_format(&format_builder);
	uint8_t buffer[4096];
	struct spa_pod_builder builder;
	struct spa_pod_frame frame[2];
	const struct spa_command command = SPA_NODE_COMMAND_INIT(SPA_NODE_COMMAND_Start);

#define RESET() spa_pod_builder_init(&builder, buffer, sizeof(buffer))

	RESET();
	spa_pod_builder_push_struct(&builder, &frame[0]);
	spa_pod_builder_add(&builder, SPA_POD_Int(1), SPA_POD_Int(0), NULL);
	spa_pod_builder_push_struct(&builder, &frame[1]);
	spa_pod_builder_add(&builder,
		SPA_POD_Int(0), SPA_POD_Int(1), SPA_POD_Long(7), SPA_POD_Long(0),
		SPA_POD_Int(0), SPA_POD_Int(0), NULL);
	spa_pod_builder_pop(&builder, &frame[1]);
	print_pod("update", spa_pod_builder_pop(&builder, &frame[0]));

	RESET();
	spa_pod_builder_push_struct(&builder, &frame[0]);
	spa_pod_builder_add(&builder,
		SPA_POD_Int(SPA_DIRECTION_OUTPUT), SPA_POD_Int(0), SPA_POD_Int(1),
		SPA_POD_Int(1), SPA_POD_Pod(format), NULL);
	spa_pod_builder_push_struct(&builder, &frame[1]);
	spa_pod_builder_add(&builder,
		SPA_POD_Long(15), SPA_POD_Long(0), SPA_POD_Int(1), SPA_POD_Int(48000),
		SPA_POD_Int(0), SPA_POD_Int(0), NULL);
	spa_pod_builder_pop(&builder, &frame[1]);
	print_pod("port-update-canonical", spa_pod_builder_pop(&builder, &frame[0]));

	RESET();
	print_pod("set-active-true",
		spa_pod_builder_add_struct(&builder, SPA_POD_Bool(true)));
	RESET();
	print_pod("set-active-false",
		spa_pod_builder_add_struct(&builder, SPA_POD_Bool(false)));

	RESET();
	print_pod("transport", spa_pod_builder_add_struct(&builder,
		SPA_POD_Fd(0), SPA_POD_Fd(1), SPA_POD_Int(4), SPA_POD_Int(0),
		SPA_POD_Int(2312)));

	RESET();
	print_pod("port-set-param", spa_pod_builder_add_struct(&builder,
		SPA_POD_Int(SPA_DIRECTION_OUTPUT), SPA_POD_Int(0),
		SPA_POD_Id(SPA_PARAM_Format), SPA_POD_Int(0), SPA_POD_Pod(format)));

	RESET();
	spa_pod_builder_push_struct(&builder, &frame[0]);
	spa_pod_builder_add(&builder,
		SPA_POD_Int(SPA_DIRECTION_OUTPUT), SPA_POD_Int(0),
		SPA_POD_Int(SPA_ID_INVALID), SPA_POD_Int(0), SPA_POD_Int(1),
		SPA_POD_Int(11), SPA_POD_Int(64), SPA_POD_Int(128), SPA_POD_Int(1),
		SPA_POD_Id(1), SPA_POD_Int(16), SPA_POD_Int(1), SPA_POD_Id(3),
		SPA_POD_Int(12), SPA_POD_Int(1), SPA_POD_Int(32), SPA_POD_Int(4096), NULL);
	print_pod("port-use-buffers", spa_pod_builder_pop(&builder, &frame[0]));

	RESET();
	print_pod("port-set-io", spa_pod_builder_add_struct(&builder,
		SPA_POD_Int(SPA_DIRECTION_OUTPUT), SPA_POD_Int(0),
		SPA_POD_Int(SPA_ID_INVALID), SPA_POD_Id(SPA_IO_Buffers),
		SPA_POD_Int(13), SPA_POD_Int(8), SPA_POD_Int(8)));

	RESET();
	print_pod("set-activation", spa_pod_builder_add_struct(&builder,
		SPA_POD_Int(55), SPA_POD_Fd(0), SPA_POD_Int(8), SPA_POD_Int(16),
		SPA_POD_Int(2312)));

	RESET();
	print_pod("command-start",
		spa_pod_builder_add_struct(&builder, SPA_POD_Pod(&command)));

	print_pod("format-s16le-48k-stereo", format);
	return 0;
}
